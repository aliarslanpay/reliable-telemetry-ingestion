#include "telemetry/outbox.hpp"
#include <filesystem>
#include <iostream>
#include <unistd.h>

namespace {
void check(bool b,const char* message) {if(!b) throw std::runtime_error(message);}
telemetry::Ack ack(const telemetry::Event& e,std::string result="stored") {
    return {e.device_id,e.stream_id,e.sequence,e.fingerprint,std::move(result)};
}
}
int main() {
    const auto path=std::filesystem::temp_directory_path()/ ("outbox-test-"+std::to_string(getpid()));
    std::filesystem::create_directory(path);
    try {
        const auto db=(path/"outbox.db").string();
        telemetry::Event first;
        {
            telemetry::Outbox outbox(db,"sensor-a",telemetry::Limits{3,4096,1,4096});
            auto r=outbox.enqueue(1000,-1000,101325);check(r.accepted,"enqueue commit");first=*r.event;
            auto wrong=ack(first);wrong.fingerprint=std::string(64,'0');
            check(outbox.apply_ack(wrong)==telemetry::AckResult::ignored,"wrong fingerprint removed event");
            wrong=ack(first);wrong.device_id="sensor-b";
            check(outbox.apply_ack(wrong)==telemetry::AckResult::ignored,"wrong device removed event");
            wrong=ack(first);wrong.sequence++;
            check(outbox.apply_ack(wrong)==telemetry::AckResult::ignored,"unknown identity removed event");
            check(outbox.pending(1).size()==1,"ignored ACK durable state");
        }
        {
            telemetry::Outbox outbox(db,"sensor-a");
            check(outbox.pending(1)[0].wire()==first.wire(),"reopen identity/data changed");
            auto second=outbox.enqueue(2000,1000,101000);check(second.accepted,"second acceptance");
            check(second.event->sequence==2 && second.event->stream_id==first.stream_id,"persistent allocation");
            check(outbox.apply_ack(ack(first,"conflict"))==telemetry::AckResult::quarantined,"quarantine");
            check(outbox.apply_ack(ack(*second.event,"invalid"))==telemetry::AckResult::quarantine_full,"quarantine capacity guard");
            auto third=outbox.enqueue(3000,2000,101000);check(third.accepted,"unrelated valid capacity");
            auto fourth=outbox.enqueue(4000,2000,101000);check(!fourth.accepted && fourth.reason=="item_capacity","item guard not reached");
            check(outbox.status()["quarantine"]==1 && outbox.status()["blocked_terminal"]==1,"terminal state lost");
            check(outbox.apply_ack(ack(*third.event))==telemetry::AckResult::acknowledged,"valid ACK");
            check(outbox.pending(1).empty(),"pending did not drain");
            check(outbox.discard_terminal(first.stream_id,first.sequence),"explicit discard");
            const auto new_stream=outbox.start_stream();
            auto next=outbox.enqueue(5000,1000,100000);
            check(next.accepted && next.event->sequence==1 && next.event->stream_id==new_stream && new_stream!=first.stream_id,"new stream");
            outbox.checkpoint(true);
        }
        {
            telemetry::Outbox bytes((path/"bytes.db").string(),"sensor-b",telemetry::Limits{10,400,1,4096});
            check(bytes.enqueue(1000,1,1000).accepted,"payload guard positive case");
            auto rejected=bytes.enqueue(1000,1,1000);
            check(!rejected.accepted && rejected.reason=="payload_capacity","payload guard not reached");
            check(bytes.status()["next_sequence"]==2 && bytes.status()["items"]==1,"rejected input advanced sequence");
        }
        {
            const auto busy_path=(path/"busy.db").string();
            telemetry::Outbox outbox(busy_path,"sensor-a");
            sqlite3* lock=nullptr;check(sqlite3_open(busy_path.c_str(),&lock)==SQLITE_OK,"lock connection");
            check(sqlite3_exec(lock,"BEGIN IMMEDIATE",nullptr,nullptr,nullptr)==SQLITE_OK,"write lock");
            auto result=outbox.enqueue(1000,1,1000);
            check(!result.accepted && result.reason=="storage_busy","busy guard not reached");
            sqlite3_exec(lock,"ROLLBACK",nullptr,nullptr,nullptr);sqlite3_close(lock);
            check(outbox.enqueue(1000,1,1000).event->sequence==1,"busy input consumed identity");
        }
        {
            telemetry::Outbox full((path/"full.db").string(),"sensor-a",telemetry::Limits{1000,1048576,1,8});
            bool reached=false;std::int64_t accepted=0;
            for(int i=0;i<1000;++i) {
                auto result=full.enqueue(1000+i,1,1000);
                if(!result.accepted) {check(result.reason=="storage_full","wrong full failure");reached=true;break;}
                ++accepted;
            }
            check(reached && accepted>0,"SQLite FULL guard not reached");
            check(full.status()["items"]==accepted && full.status()["next_sequence"]==accepted+1,"FULL rollback lost accepted rows");
        }
        std::filesystem::remove_all(path);
        std::cout<<"outbox capacity, ACK guards, terminal limits, busy/FULL rollback and reopen passed\n";
    } catch(const std::exception& e) {std::cerr<<e.what()<<'\n';std::filesystem::remove_all(path);return 1;}
}
