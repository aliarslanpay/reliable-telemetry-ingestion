#include "telemetry/outbox.hpp"
#include <algorithm>
#include <chrono>
#include <utility>

namespace telemetry {
namespace {
[[noreturn]] void fail(sqlite3* db, int rc) { throw StorageError(rc, sqlite3_errmsg(db)); }
void exec(sqlite3* db, const std::string& sql) {
    const int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, nullptr);
    if (rc != SQLITE_OK) fail(db, rc);
}
class Statement {
    sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
public:
    Statement(sqlite3* db, const char* sql) : db_(db) {
        const int rc = sqlite3_prepare_v2(db_, sql, -1, &stmt_, nullptr);
        if (rc != SQLITE_OK) fail(db_, rc);
    }
    ~Statement() { sqlite3_finalize(stmt_); }
    void text(int i, std::string_view s) {
        const int rc = sqlite3_bind_text(stmt_, i, s.data(), static_cast<int>(s.size()), SQLITE_TRANSIENT);
        if (rc != SQLITE_OK) fail(db_, rc);
    }
    void number(int i, std::int64_t n) { const int rc = sqlite3_bind_int64(stmt_, i, n); if (rc != SQLITE_OK) fail(db_, rc); }
    bool row() {
        const int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) return true;
        if (rc != SQLITE_DONE) fail(db_, rc);
        return false;
    }
    void done() { if (row()) throw std::runtime_error("unexpected query row"); }
    std::int64_t number(int col) const { return sqlite3_column_int64(stmt_, col); }
    std::string text(int col) const {
        const auto* s = sqlite3_column_text(stmt_, col);
        return s ? reinterpret_cast<const char*>(s) : "";
    }
};
class Transaction {
    sqlite3* db_;
    bool committed_ = false;
public:
    explicit Transaction(sqlite3* db) : db_(db) { exec(db_, "BEGIN IMMEDIATE"); }
    ~Transaction() { if (!committed_) sqlite3_exec(db_, "ROLLBACK", nullptr, nullptr, nullptr); }
    void commit() { exec(db_, "COMMIT"); committed_ = true; }
};
std::string storage_reason(int code) {
    switch (code & 255) {
    case SQLITE_BUSY: case SQLITE_LOCKED: return "storage_busy";
    case SQLITE_FULL: return "storage_full";
    case SQLITE_IOERR: return "storage_io";
    case SQLITE_READONLY: return "storage_readonly";
    default: return "storage_error";
    }
}
void validate_limits(const Limits& l) {
    if (l.items < 1 || l.items > 100000 || l.payload_bytes < 1 || l.payload_bytes > 67108864 ||
        l.quarantine < 0 || l.quarantine > l.items || l.database_pages < 8 || l.database_pages > 65536)
        throw std::invalid_argument("invalid outbox limits");
}
}

StorageError::StorageError(int rc, const std::string& msg) : std::runtime_error(msg), code(rc) {}
std::int64_t wall_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}
Outbox::Outbox(const std::string& path, std::string device, std::optional<Limits> requested) : device_(std::move(device)) {
    if (!valid_device(device_)) throw std::invalid_argument("invalid device ID");
    if (requested) validate_limits(*requested);
    int rc = sqlite3_open_v2(path.c_str(), &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, nullptr);
    if (rc != SQLITE_OK) {
        const std::string error = db_ ? sqlite3_errmsg(db_) : "SQLite open failed";
        sqlite3_close(db_); db_ = nullptr;
        throw StorageError(rc, error);
    }
    try {
        sqlite3_extended_result_codes(db_, 1);
        sqlite3_busy_timeout(db_, 150);
        sqlite3_limit(db_, SQLITE_LIMIT_LENGTH, 4096);
        exec(db_, "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=32; PRAGMA journal_size_limit=262144;");
        Transaction tx(db_);
        exec(db_, "CREATE TABLE IF NOT EXISTS meta(singleton INTEGER PRIMARY KEY CHECK(singleton=1), schema_version INTEGER NOT NULL CHECK(schema_version=1), device TEXT NOT NULL, stream TEXT NOT NULL, next_seq INTEGER NOT NULL, max_items INTEGER NOT NULL, max_bytes INTEGER NOT NULL, max_quarantine INTEGER NOT NULL, max_pages INTEGER NOT NULL);"
                  "CREATE TABLE IF NOT EXISTS outbox(stream TEXT NOT NULL, seq INTEGER NOT NULL, wire TEXT NOT NULL, fingerprint TEXT NOT NULL, enqueued_ms INTEGER NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','quarantine','blocked')), reason TEXT NOT NULL DEFAULT '', PRIMARY KEY(stream,seq));");
        Statement read(db_, "SELECT device,max_items,max_bytes,max_quarantine,max_pages,schema_version FROM meta WHERE singleton=1");
        if (read.row()) {
            if (read.text(0) != device_ || read.number(5) != 1) throw std::invalid_argument("outbox identity/schema mismatch");
            limits_ = {read.number(1), read.number(2), read.number(3), read.number(4)};
            validate_limits(limits_);
            if (requested && *requested != limits_) throw std::invalid_argument("outbox limits differ from persistent configuration");
        } else {
            limits_ = requested.value_or(Limits{});
            validate_limits(limits_);
            Statement insert(db_, "INSERT INTO meta VALUES(1,1,?,?,1,?,?,?,?)");
            insert.text(1, device_); insert.text(2, new_stream_id());
            insert.number(3, limits_.items); insert.number(4, limits_.payload_bytes);
            insert.number(5, limits_.quarantine); insert.number(6, limits_.database_pages); insert.done();
        }
        tx.commit();
        exec(db_, "PRAGMA max_page_count=" + std::to_string(limits_.database_pages));
    } catch (...) { sqlite3_close(db_); db_ = nullptr; throw; }
}
Outbox::~Outbox() { if (db_) sqlite3_close(db_); }
EnqueueResult Outbox::enqueue(std::int64_t timestamp, std::int64_t temperature, std::int64_t pressure) {
    try {
        Event e{device_, std::string(32, '0'), 1, timestamp, temperature, pressure, ""};
        validate_data(e);
        Transaction tx(db_);
        Statement meta(db_, "SELECT stream,next_seq FROM meta WHERE singleton=1");
        if (!meta.row()) throw std::runtime_error("outbox metadata missing");
        e.stream_id = meta.text(0); e.sequence = meta.number(1);
        validate_data(e);
        e.fingerprint = sha256(e.canonical());
        const std::string wire = e.wire();
        Statement capacity(db_, "SELECT count(*),coalesce(sum(length(CAST(wire AS BLOB))),0) FROM outbox");
        capacity.row();
        if (capacity.number(0) >= limits_.items) return {false, "item_capacity", std::nullopt};
        if (capacity.number(1) + static_cast<std::int64_t>(wire.size()) > limits_.payload_bytes)
            return {false, "payload_capacity", std::nullopt};
        Statement insert(db_, "INSERT INTO outbox(stream,seq,wire,fingerprint,enqueued_ms,state) VALUES(?,?,?,?,?,'pending')");
        insert.text(1, e.stream_id); insert.number(2, e.sequence); insert.text(3, wire);
        insert.text(4, e.fingerprint); insert.number(5, wall_ms()); insert.done();
        exec(db_, "UPDATE meta SET next_seq=next_seq+1 WHERE singleton=1");
        tx.commit();
        if (++changes_ % 32 == 0) {
            try { checkpoint(); } catch (const StorageError& error) { checkpoint_error_ = error.code; }
        }
        return {true, "accepted", std::move(e)};
    } catch (const StorageError& e) { return {false, storage_reason(e.code), std::nullopt}; }
      catch (const std::invalid_argument&) { return {false, "invalid_input", std::nullopt}; }
}
std::vector<Event> Outbox::pending(std::size_t limit) const {
    if (limit < 1 || limit > 64) throw std::invalid_argument("pending batch limit");
    Statement rows(db_, "SELECT wire FROM outbox WHERE state='pending' ORDER BY enqueued_ms,stream,seq LIMIT ?");
    rows.number(1, static_cast<std::int64_t>(limit));
    std::vector<Event> result;
    while (rows.row()) result.push_back(parse_event(rows.text(0)));
    return result;
}
AckResult Outbox::apply_ack(const Ack& ack) {
    if (ack.device_id != device_) return AckResult::ignored;
    Transaction tx(db_);
    Statement row(db_, "SELECT fingerprint,state FROM outbox WHERE stream=? AND seq=?");
    row.text(1, ack.stream_id); row.number(2, ack.sequence);
    if (!row.row() || row.text(0) != ack.fingerprint || row.text(1) != "pending") return AckResult::ignored;
    if (ack.result == "stored" || ack.result == "duplicate") {
        Statement del(db_, "DELETE FROM outbox WHERE stream=? AND seq=?");
        del.text(1, ack.stream_id); del.number(2, ack.sequence); del.done();
        tx.commit();
        if (++changes_ % 32 == 0) {
            try { checkpoint(); } catch (const StorageError& error) { checkpoint_error_ = error.code; }
        }
        return AckResult::acknowledged;
    }
    if (ack.result != "invalid" && ack.result != "conflict") return AckResult::ignored;
    Statement count(db_, "SELECT count(*) FROM outbox WHERE state='quarantine'"); count.row();
    const bool full = count.number(0) >= limits_.quarantine;
    Statement update(db_, "UPDATE outbox SET state=?,reason=? WHERE stream=? AND seq=?");
    update.text(1, full ? "blocked" : "quarantine"); update.text(2, ack.result);
    update.text(3, ack.stream_id); update.number(4, ack.sequence); update.done(); tx.commit();
    return full ? AckResult::quarantine_full : AckResult::quarantined;
}
Json Outbox::status() const {
    Statement stats(db_, "SELECT count(*),coalesce(sum(length(CAST(wire AS BLOB))),0),coalesce(sum(state='pending'),0),coalesce(sum(state='quarantine'),0),coalesce(sum(state='blocked'),0),coalesce(min(CASE WHEN state='pending' THEN enqueued_ms END),0) FROM outbox");
    stats.row();
    Statement meta(db_, "SELECT stream,next_seq FROM meta WHERE singleton=1"); meta.row();
    const auto oldest = stats.number(5);
    return {{"device_id",device_},{"stream_id",meta.text(0)},{"next_sequence",meta.number(1)},
            {"items",stats.number(0)},{"payload_bytes",stats.number(1)},{"pending",stats.number(2)},
            {"quarantine",stats.number(3)},{"blocked_terminal",stats.number(4)},
            {"oldest_pending_age_ms", oldest ? std::max<std::int64_t>(0, wall_ms()-oldest) : 0},
            {"max_items",limits_.items},{"max_payload_bytes",limits_.payload_bytes},{"max_quarantine",limits_.quarantine},
            {"max_database_pages",limits_.database_pages},{"checkpoint_error_code",checkpoint_error_}};
}
Json Outbox::terminal(std::size_t limit) const {
    if (limit < 1 || limit > 100) throw std::invalid_argument("terminal inspection limit");
    Statement rows(db_, "SELECT wire,state,reason FROM outbox WHERE state!='pending' ORDER BY enqueued_ms,stream,seq LIMIT ?");
    rows.number(1, static_cast<std::int64_t>(limit));
    Json result = Json::array();
    while (rows.row()) result.push_back({{"event",parse_bounded(rows.text(0),max_event_bytes)}, {"state",rows.text(1)},{"reason",rows.text(2)}});
    return result;
}
bool Outbox::discard_terminal(std::string_view stream, std::int64_t sequence) {
    Transaction tx(db_);
    Statement del(db_, "DELETE FROM outbox WHERE stream=? AND seq=? AND state!='pending'");
    del.text(1, stream); del.number(2, sequence); del.done();
    const bool removed = sqlite3_changes(db_) == 1; tx.commit(); return removed;
}
std::string Outbox::start_stream() {
    Transaction tx(db_);
    auto stream = new_stream_id();
    Statement update(db_, "UPDATE meta SET stream=?,next_seq=1 WHERE singleton=1");
    update.text(1, stream); update.done(); tx.commit(); return stream;
}
bool Outbox::checkpoint(bool truncate) {
    const int rc = sqlite3_wal_checkpoint_v2(db_, nullptr, truncate ? SQLITE_CHECKPOINT_TRUNCATE : SQLITE_CHECKPOINT_PASSIVE, nullptr, nullptr);
    if (rc == SQLITE_BUSY || (rc & 255) == SQLITE_LOCKED) return false;
    if (rc != SQLITE_OK) fail(db_, rc);
    return true;
}
} // namespace telemetry
