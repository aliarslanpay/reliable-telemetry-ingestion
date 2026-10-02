#pragma once
#include <cstdint>
#include <string>
#include <string_view>
#include <nlohmann/json.hpp>

namespace telemetry {
using Json = nlohmann::json;
constexpr std::size_t max_event_bytes = 1024;
constexpr std::size_t max_ack_bytes = 512;
constexpr std::int64_t max_sequence = 9007199254740991LL;
constexpr std::int64_t max_timestamp_ms = 253402300799999LL;

struct Event {
    std::string device_id;
    std::string stream_id;
    std::int64_t sequence{};
    std::int64_t timestamp_ms{};
    std::int64_t temperature_mc{};
    std::int64_t pressure_pa{};
    std::string fingerprint;
    Json data() const;
    std::string canonical() const;
    std::string wire() const;
};

struct Ack {
    std::string device_id;
    std::string stream_id;
    std::int64_t sequence{};
    std::string fingerprint;
    std::string result;
};

bool valid_device(std::string_view value);
bool lower_hex(std::string_view value, std::size_t length);
std::string sha256(std::string_view value);
Json parse_bounded(std::string_view value, std::size_t limit);
Event parse_event(std::string_view value);
Ack parse_ack(std::string_view value);
void validate_data(const Event& event);
std::string new_stream_id();
} // namespace telemetry
