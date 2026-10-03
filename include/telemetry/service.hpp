#pragma once
#include "telemetry/mqtt.hpp"
#include "telemetry/outbox.hpp"
#include <csignal>

namespace telemetry {
struct RunConfig {
    MqttConfig mqtt;
    std::int64_t duration_ms=30000;
    std::int64_t ack_timeout_ms=1000;
    std::int64_t interval_ms=0;
    std::int64_t count=0;
    std::int64_t timestamp_ms=0;
    unsigned window=8;
    bool stay=false;
    std::string replay;
};
int run_service(Outbox& outbox,const RunConfig& config,volatile std::sig_atomic_t& stop);
} // namespace telemetry
