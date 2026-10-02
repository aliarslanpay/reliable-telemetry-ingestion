#include "telemetry/contract.hpp"
#include <array>
#include <memory>
#include <set>
#include <stdexcept>
#include <openssl/evp.h>
#include <openssl/rand.h>

namespace telemetry {
namespace {
std::string hex(const unsigned char* bytes, std::size_t count) {
    constexpr char alphabet[] = "0123456789abcdef";
    std::string result(count * 2, '0');
    for (std::size_t i = 0; i < count; ++i) {
        result[2 * i] = alphabet[bytes[i] >> 4];
        result[2 * i + 1] = alphabet[bytes[i] & 15];
    }
    return result;
}
std::int64_t integer(const Json& j, const char* key, std::int64_t lo, std::int64_t hi) {
    const auto& v = j.at(key);
    if (!v.is_number_integer() || (v.is_number_unsigned() && v.get<std::uint64_t>() > static_cast<std::uint64_t>(hi)))
        throw std::invalid_argument("invalid integer");
    const auto n = v.get<std::int64_t>();
    if (n < lo || n > hi) throw std::invalid_argument("integer outside range");
    return n;
}
void keys(const Json& j, std::initializer_list<const char*> names) {
    if (!j.is_object() || j.size() != names.size()) throw std::invalid_argument("unexpected fields");
    for (const auto* name : names) if (!j.contains(name)) throw std::invalid_argument("missing field");
}
void identity(std::string_view device, std::string_view stream, std::int64_t seq) {
    if (!valid_device(device) || !lower_hex(stream, 32) || seq < 1 || seq > max_sequence)
        throw std::invalid_argument("invalid identity");
}
}

bool valid_device(std::string_view s) {
    if (s.empty() || s.size() > 32) return false;
    const auto alnum = [](char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    if (!alnum(s.front())) return false;
    for (char c : s) if (!alnum(c) && c != '_' && c != '-') return false;
    return true;
}
bool lower_hex(std::string_view s, std::size_t n) {
    if (s.size() != n) return false;
    for (char c : s) if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    return true;
}
std::string sha256(std::string_view s) {
    std::array<unsigned char, EVP_MAX_MD_SIZE> digest{};
    unsigned int length = 0;
    if (EVP_Digest(s.data(), s.size(), digest.data(), &length, EVP_sha256(), nullptr) != 1)
        throw std::runtime_error("SHA-256 failed");
    return hex(digest.data(), length);
}
std::string new_stream_id() {
    std::array<unsigned char, 16> bytes{};
    if (RAND_bytes(bytes.data(), bytes.size()) != 1) throw std::runtime_error("random stream ID failed");
    return hex(bytes.data(), bytes.size());
}
Json parse_bounded(std::string_view s, std::size_t limit) {
    if (s.empty() || s.size() > limit) throw std::invalid_argument("message size");
    std::vector<std::set<std::string>> objects;
    auto callback = [&objects](int depth, Json::parse_event_t event, Json& value) {
        if (depth > 4) throw std::invalid_argument("nesting depth");
        if (event == Json::parse_event_t::object_start) objects.emplace_back();
        if (event == Json::parse_event_t::key && !objects.back().insert(value.get<std::string>()).second)
            throw std::invalid_argument("duplicate key");
        if (event == Json::parse_event_t::object_end) objects.pop_back();
        return true;
    };
    return Json::parse(s.begin(), s.end(), callback);
}
Json Event::data() const {
    return {{"schema_version", 1}, {"device_id", device_id}, {"stream_id", stream_id},
            {"sequence", sequence}, {"timestamp_ms", timestamp_ms},
            {"temperature_mc", temperature_mc}, {"pressure_pa", pressure_pa}};
}
std::string Event::canonical() const { return data().dump(); }
std::string Event::wire() const { auto j = data(); j["fingerprint"] = fingerprint; return j.dump(); }
void validate_data(const Event& e) {
    identity(e.device_id, e.stream_id, e.sequence);
    if (e.timestamp_ms < 0 || e.timestamp_ms > max_timestamp_ms || e.temperature_mc < -100000 ||
        e.temperature_mc > 200000 || e.pressure_pa < 0 || e.pressure_pa > 2000000)
        throw std::invalid_argument("measurement outside range");
}
Event parse_event(std::string_view s) {
    auto j = parse_bounded(s, max_event_bytes);
    keys(j, {"schema_version", "device_id", "stream_id", "sequence", "timestamp_ms",
             "temperature_mc", "pressure_pa", "fingerprint"});
    integer(j, "schema_version", 1, 1);
    Event e{j.at("device_id").get<std::string>(), j.at("stream_id").get<std::string>(),
            integer(j, "sequence", 1, max_sequence), integer(j, "timestamp_ms", 0, max_timestamp_ms),
            integer(j, "temperature_mc", -100000, 200000), integer(j, "pressure_pa", 0, 2000000),
            j.at("fingerprint").get<std::string>()};
    validate_data(e);
    if (!lower_hex(e.fingerprint, 64) || e.fingerprint != sha256(e.canonical()))
        throw std::invalid_argument("fingerprint mismatch");
    return e;
}
Ack parse_ack(std::string_view s) {
    auto j = parse_bounded(s, max_ack_bytes);
    keys(j, {"schema_version", "device_id", "stream_id", "sequence", "fingerprint", "result"});
    integer(j, "schema_version", 1, 1);
    Ack a{j.at("device_id").get<std::string>(), j.at("stream_id").get<std::string>(),
          integer(j, "sequence", 1, max_sequence), j.at("fingerprint").get<std::string>(),
          j.at("result").get<std::string>()};
    identity(a.device_id, a.stream_id, a.sequence);
    if (!lower_hex(a.fingerprint, 64) || (a.result != "stored" && a.result != "duplicate" &&
        a.result != "invalid" && a.result != "conflict")) throw std::invalid_argument("invalid ACK");
    return a;
}
} // namespace telemetry
