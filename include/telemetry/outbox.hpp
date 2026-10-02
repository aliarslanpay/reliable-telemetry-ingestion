#pragma once
#include "telemetry/contract.hpp"
#include <optional>
#include <stdexcept>
#include <vector>
#include <sqlite3.h>

namespace telemetry {
struct Limits {
    std::int64_t items = 4096;
    std::int64_t payload_bytes = 1048576;
    std::int64_t quarantine = 64;
    std::int64_t database_pages = 4096;
    bool operator==(const Limits&) const = default;
};
class StorageError : public std::runtime_error {
public:
    int code;
    StorageError(int code, const std::string& message);
};
struct EnqueueResult {
    bool accepted = false;
    std::string reason;
    std::optional<Event> event;
};
enum class AckResult { ignored, acknowledged, quarantined, quarantine_full };
class Outbox {
    sqlite3* db_ = nullptr;
    std::string device_;
    Limits limits_;
    unsigned changes_ = 0;
    int checkpoint_error_ = 0;
public:
    Outbox(const std::string& path, std::string device, std::optional<Limits> limits = std::nullopt);
    ~Outbox();
    Outbox(const Outbox&) = delete;
    Outbox& operator=(const Outbox&) = delete;
    EnqueueResult enqueue(std::int64_t timestamp_ms, std::int64_t temperature_mc, std::int64_t pressure_pa);
    std::vector<Event> pending(std::size_t limit) const;
    AckResult apply_ack(const Ack& ack);
    Json status() const;
    Json terminal(std::size_t limit) const;
    bool discard_terminal(std::string_view stream, std::int64_t sequence);
    std::string start_stream();
    bool checkpoint(bool truncate = false);
};
std::int64_t wall_ms();
} // namespace telemetry
