#pragma once
#include "telemetry/handoff.hpp"
#include <atomic>
#include <chrono>
#include <string>
#include <mosquitto.h>

namespace telemetry {
struct MqttConfig {
    std::string host="127.0.0.1";
    int port=1883;
    std::string device;
    std::string username;
    std::string password;
    std::string ca_file;
    std::string cert_file;
    std::string key_file;
    unsigned wire_window=16;
};
class MqttBridge {
    mosquitto* client_=nullptr;
    bool loop_started_=false;
    std::chrono::steady_clock::time_point next_connect_{};
    const MqttConfig config_;
    const std::string event_topic_;
    const std::string ack_topic_;
    Handoff<std::string> acks_{64};
    std::atomic<bool> subscribe_{false};
    std::atomic<bool> ready_{false};
    std::atomic<unsigned> outstanding_{0};
    std::atomic<unsigned long long> dropped_{0};
    std::atomic<int> transport_code_{0};
    void start_connection();
    static void connected(mosquitto*,void*,int) noexcept;
    static void disconnected(mosquitto*,void*,int) noexcept;
    static void subscribed(mosquitto*,void*,int,int,const int*) noexcept;
    static void published(mosquitto*,void*,int) noexcept;
    static void message(mosquitto*,void*,const mosquitto_message*) noexcept;
public:
    explicit MqttBridge(MqttConfig config);
    ~MqttBridge();
    MqttBridge(const MqttBridge&)=delete;
    MqttBridge& operator=(const MqttBridge&)=delete;
    void poll_subscription();
    bool send(const std::string& payload);
    std::optional<std::string> ack() {return acks_.pop();}
    bool ready() const {return ready_.load(std::memory_order_acquire);}
    unsigned long long dropped() const {return dropped_.load();}
    unsigned outstanding() const {return outstanding_.load();}
    int transport_code() const {return transport_code_.load();}
};
} // namespace telemetry
