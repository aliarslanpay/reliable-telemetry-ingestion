#include "telemetry/mqtt.hpp"
#include "telemetry/contract.hpp"
#include <stdexcept>
#include <utility>

namespace telemetry {
namespace {
void must(int code) {if(code!=MOSQ_ERR_SUCCESS) throw std::runtime_error(mosquitto_strerror(code));}
}
MqttBridge::MqttBridge(MqttConfig config) : config_(std::move(config)),
    event_topic_("telemetry/v1/"+config_.device+"/events"),ack_topic_("telemetry/v1/"+config_.device+"/acks") {
    if(!valid_device(config_.device) || config_.port<1 || config_.port>65535 ||
        config_.wire_window<1 || config_.wire_window>64) throw std::invalid_argument("MQTT configuration");
    must(mosquitto_lib_init());
    try {
        // The stable client ID identifies a device connection, not an event.
        client_=mosquitto_new(config_.device.c_str(),true,this);
        if(!client_) throw std::runtime_error("MQTT allocation failed");
        must(mosquitto_int_option(client_,MOSQ_OPT_SEND_MAXIMUM,static_cast<int>(config_.wire_window)));
        must(mosquitto_int_option(client_,MOSQ_OPT_TCP_NODELAY,1));
        if(!config_.username.empty()) must(mosquitto_username_pw_set(client_,config_.username.c_str(),config_.password.c_str()));
        if(!config_.ca_file.empty()) {
            must(mosquitto_tls_set(client_,config_.ca_file.c_str(),nullptr,
                config_.cert_file.empty()?nullptr:config_.cert_file.c_str(),
                config_.key_file.empty()?nullptr:config_.key_file.c_str(),nullptr));
            must(mosquitto_tls_opts_set(client_,1,"tlsv1.2",nullptr));
        } else if(!config_.cert_file.empty() || !config_.key_file.empty()) throw std::invalid_argument("TLS certificate requires CA");
        mosquitto_connect_callback_set(client_,connected);
        mosquitto_disconnect_callback_set(client_,disconnected);
        mosquitto_subscribe_callback_set(client_,subscribed);
        mosquitto_publish_callback_set(client_,published);
        mosquitto_message_callback_set(client_,message);
        must(mosquitto_reconnect_delay_set(client_,1,4,true));
        start_connection();
    } catch(...) {if(client_) mosquitto_destroy(client_);mosquitto_lib_cleanup();throw;}
}
MqttBridge::~MqttBridge() {
    if(client_) {
        ready_.store(false);
        mosquitto_disconnect(client_);
        // Userdata and handoff remain alive until all callbacks have stopped.
        if(loop_started_) mosquitto_loop_stop(client_,false);
        mosquitto_destroy(client_);
    }
    mosquitto_lib_cleanup();
}
void MqttBridge::connected(mosquitto*,void* context,int code) noexcept {
    auto& self=*static_cast<MqttBridge*>(context);
    self.ready_.store(false,std::memory_order_release);
    self.subscribe_.store(code==0,std::memory_order_release);
}
void MqttBridge::disconnected(mosquitto*,void* context,int) noexcept {
    auto& self=*static_cast<MqttBridge*>(context);
    self.ready_.store(false,std::memory_order_release);
}
void MqttBridge::subscribed(mosquitto*,void* context,int,int count,const int* granted) noexcept {
    auto& self=*static_cast<MqttBridge*>(context);
    self.ready_.store(count==1 && granted && granted[0]>=0 && granted[0]<=2,std::memory_order_release);
}
void MqttBridge::published(mosquitto*,void* context,int) noexcept {
    auto& self=*static_cast<MqttBridge*>(context);
    auto n=self.outstanding_.load();
    while(n && !self.outstanding_.compare_exchange_weak(n,n-1)) {}
}
void MqttBridge::message(mosquitto*,void* context,const mosquitto_message* msg) noexcept {
    auto& self=*static_cast<MqttBridge*>(context);
    if(!msg || !msg->topic || msg->retain || msg->qos!=1 || msg->payloadlen<=0 ||
        static_cast<std::size_t>(msg->payloadlen)>max_ack_bytes || self.ack_topic_!=msg->topic) {
        ++self.dropped_;return;
    }
    try {
        if(!self.acks_.push(std::string(static_cast<const char*>(msg->payload),static_cast<std::size_t>(msg->payloadlen)))) ++self.dropped_;
    } catch(...) {++self.dropped_;}
}
void MqttBridge::poll_subscription() {
    if(!loop_started_ && std::chrono::steady_clock::now()>=next_connect_) start_connection();
    if(subscribe_.exchange(false)) {
        const int rc=mosquitto_subscribe(client_,nullptr,ack_topic_.c_str(),1);
        if(rc!=MOSQ_ERR_SUCCESS) subscribe_.store(true);
    }
}
void MqttBridge::start_connection() {
    const int rc=mosquitto_connect_async(client_,config_.host.c_str(),config_.port,10);
    transport_code_.store(rc);
    if(rc==MOSQ_ERR_SUCCESS) {
        must(mosquitto_loop_start(client_)); loop_started_=true;
    } else if(rc==MOSQ_ERR_ERRNO || rc==MOSQ_ERR_EAI) {
        // No network thread exists yet; persist input while bounded startup retries run.
        next_connect_=std::chrono::steady_clock::now()+std::chrono::seconds(1);
    } else must(rc);
}
bool MqttBridge::send(const std::string& payload) {
    if(payload.empty() || payload.size()>max_event_bytes || !ready()) return false;
    const unsigned before=outstanding_.fetch_add(1);
    if(before>=config_.wire_window) {--outstanding_;return false;}
    const int rc=mosquitto_publish(client_,nullptr,event_topic_.c_str(),static_cast<int>(payload.size()),payload.data(),1,false);
    if(rc!=MOSQ_ERR_SUCCESS) {--outstanding_;return false;}
    return true;
}
} // namespace telemetry
