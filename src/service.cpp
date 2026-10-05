#include "telemetry/service.hpp"
#include "telemetry/input.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <thread>

namespace telemetry {
namespace {
using Clock=std::chrono::steady_clock;
struct Flight {
    Event event;
    unsigned attempts=0;
    Clock::time_point first{},last{},due{};
    bool waiting=false;
};
std::string key(std::string_view stream,std::int64_t seq) {return std::string(stream)+":"+std::to_string(seq);}
void log(const Json& entry) {std::cout<<entry.dump()<<std::endl;}
std::optional<std::string> line(std::ifstream& stream) {
    std::string result;
    char c;
    bool any=false,oversize=false;
    while(stream.get(c)) {
        any=true;
        if(c=='\n') break;
        if(result.size()<max_event_bytes) result.push_back(c);else oversize=true;
    }
    if(!any) return std::nullopt;
    if(oversize) return std::string(max_event_bytes+1,'x');
    return result;
}
Event measurement(std::string_view raw) {
    auto j=parse_bounded(raw,max_event_bytes);
    if(!j.is_object() || j.size()!=3) throw std::invalid_argument("replay fields");
    for(const auto* k:{"timestamp_ms","temperature_mc","pressure_pa"}) {
        const auto& v=j.at(k);
        if(!v.is_number_integer() || (v.is_number_unsigned() && v.get<std::uint64_t>()>253402300799999ULL))
            throw std::invalid_argument("replay integer");
    }
    Event e{"sensor",std::string(32,'0'),1,j.at("timestamp_ms").get<std::int64_t>(),
        j.at("temperature_mc").get<std::int64_t>(),j.at("pressure_pa").get<std::int64_t>(),""};
    validate_data(e);return e;
}
}
int run_service(Outbox& outbox,const RunConfig& c,volatile std::sig_atomic_t& stop) {
    if(c.duration_ms<1 || c.duration_ms>120000 || c.ack_timeout_ms<50 || c.ack_timeout_ms>10000 ||
        c.window<1 || c.window>64 || c.count<0 || c.count>10000 || c.interval_ms<0 || c.interval_ms>1000)
        throw std::invalid_argument("run configuration range");
    if(c.replay.empty()) validate_timestamp_series(c.timestamp_ms,c.count);
    std::ifstream replay;
    if(!c.replay.empty()) {
        if(c.count!=0 || std::filesystem::file_size(c.replay)>4194304) throw std::invalid_argument("replay size or count");
        replay.open(c.replay);if(!replay) throw std::invalid_argument("replay open failed");
    }
    MqttBridge bridge(c.mqtt);
    const auto start=Clock::now(),deadline=start+std::chrono::milliseconds(c.duration_ms);
    auto next_input=start,next_status=start;
    std::map<std::string,Flight> flights;
    std::mt19937 jitter(0x517abcU);
    std::int64_t inputs=0,accepted=0,rejected=0,retries=0,acknowledged=0,terminal_count=0,ignored_acks=0;
    bool input_done=c.replay.empty() && c.count==0;
    while(!stop && Clock::now()<deadline) {
        auto now=Clock::now();bridge.poll_subscription();
        for(unsigned batch=0;batch<8 && !stop && !input_done && now>=next_input;++batch) {
            std::optional<Event> sample;
            try {
                if(!c.replay.empty()) {
                    auto raw=line(replay);if(!raw) {input_done=true;break;}
                    sample=measurement(*raw);
                } else sample=Event{"sensor",std::string(32,'0'),1,c.timestamp_ms+inputs,20000+(inputs%100),101325+(inputs%17),""};
                if(inputs>=10000) throw std::invalid_argument("replay input limit");
                auto result=outbox.enqueue(sample->timestamp_ms,sample->temperature_mc,sample->pressure_pa);
                Json entry={{"kind",result.accepted?"accepted":"rejected"},{"input_index",inputs},{"reason",result.reason}};
                if(result.accepted) {++accepted;entry["event"]=parse_bounded(result.event->wire(),max_event_bytes);}else ++rejected;
                log(entry);
            } catch(const std::exception&) {++rejected;log({{"kind","rejected"},{"input_index",inputs},{"reason","invalid_input"}});}
            ++inputs;next_input=now+std::chrono::milliseconds(c.interval_ms);
            if(c.replay.empty() && inputs>=c.count) input_done=true;
            if(inputs>=10000) input_done=true;
        }
        for(unsigned batch=0;batch<64;++batch) {
            auto raw=bridge.ack();if(!raw) break;
            try {
                const auto a=parse_ack(*raw);
                const auto result=outbox.apply_ack(a);
                const auto id=key(a.stream_id,a.sequence);
                if(result==AckResult::ignored) {++ignored_acks;continue;}
                Json entry={{"kind",result==AckResult::acknowledged?"acknowledged":"terminal"},
                    {"stream_id",a.stream_id},{"sequence",a.sequence},{"result",a.result}};
                const auto it=flights.find(id);
                if(result==AckResult::acknowledged) {
                    ++acknowledged;
                    if(it!=flights.end() && it->second.attempts) {
                        entry["delivery_latency_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-it->second.first).count();
                        entry["attempt_ack_latency_ms"]=std::chrono::duration<double,std::milli>(Clock::now()-it->second.last).count();
                    }
                }else {++terminal_count;entry["quarantine_full"]=result==AckResult::quarantine_full;}
                flights.erase(id);log(entry);
            } catch(const StorageError& e) {log({{"kind","ack_storage_error"},{"sqlite_code",e.code}});}
              catch(const std::exception&) {++ignored_acks;}
        }
        if(flights.size()<c.window) {
            for(auto& e:outbox.pending(64)) {
                if(flights.size()>=c.window) break;
                const auto id=key(e.stream_id,e.sequence);
                if(!flights.contains(id)) flights.emplace(id,Flight{std::move(e),0,{},{},now,false});
            }
        }
        for(auto& [id,f]:flights) {
            (void)id;
            if(f.waiting && now>=f.due) {
                f.waiting=false;
                const auto base=std::min<unsigned>(2000,100U<<std::min<unsigned>(f.attempts-1,5));
                const auto delay=std::min<unsigned>(2000,base*(80+(jitter()%41))/100);
                f.due=now+std::chrono::milliseconds(delay);
            }
            if(!f.waiting && now>=f.due && bridge.send(f.event.wire())) {
                if(f.attempts) {++retries;log({{"kind","retry"},{"stream_id",f.event.stream_id},{"sequence",f.event.sequence}});}else f.first=now;
                ++f.attempts;f.last=now;f.waiting=true;f.due=now+std::chrono::milliseconds(c.ack_timeout_ms);
            }
        }
        if(now>=next_status) {
            auto s=outbox.status();s["kind"]="status";s["connected"]=bridge.ready();s["accepted"]=accepted;s["rejected"]=rejected;
            s["retries"]=retries;s["acknowledged"]=acknowledged;s["terminal_received"]=terminal_count;
            s["ignored_acks"]=ignored_acks;s["callback_dropped"]=bridge.dropped();s["wire_outstanding"]=bridge.outstanding();
            s["mqtt_transport_code"]=bridge.transport_code();
            log(s);next_status=now+std::chrono::seconds(1);
        }
        if(input_done && !c.stay && flights.empty() && outbox.status()["pending"]==0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    outbox.checkpoint(true);
    auto final=outbox.status();final["kind"]="shutdown";final["accepted"]=accepted;final["rejected"]=rejected;
    final["acknowledged"]=acknowledged;final["retries"]=retries;log(final);
    if(stop) return 0;
    if(final["pending"]!=0) return 5;
    if(rejected) return 3;
    if(final["quarantine"]!=0 || final["blocked_terminal"]!=0) return 6;
    return 0;
}
} // namespace telemetry
