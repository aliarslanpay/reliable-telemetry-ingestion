#pragma once
#include "telemetry/contract.hpp"
#include <stdexcept>

namespace telemetry {
inline void validate_timestamp_series(std::int64_t first, std::int64_t count) {
    if (first < 0 || first > max_timestamp_ms || count < 0)
        throw std::invalid_argument("timestamp-ms range 0..253402300799999");
    if (count > 0 && count - 1 > max_timestamp_ms - first)
        throw std::invalid_argument("timestamp-ms series exceeds contract range");
}
} // namespace telemetry
