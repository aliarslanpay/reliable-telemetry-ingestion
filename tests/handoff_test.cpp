#include "telemetry/handoff.hpp"
#include <atomic>
#include <iostream>
#include <latch>
#include <set>
#include <thread>
#include <vector>

int main() {
    try {
        telemetry::Handoff<int> guard(1);
        if(!guard.push(1) || guard.push(2) || guard.pop()!=1 || guard.pop()) throw std::runtime_error("handoff capacity/order");
        telemetry::Handoff<int> queue(64);
        std::latch start(5);
        std::vector<std::thread> producers;
        for(int producer=0;producer<4;++producer) producers.emplace_back([&,producer] {
            start.count_down();start.wait();
            for(int i=0;i<1000;++i) while(!queue.push(producer*1000+i)) std::this_thread::yield();
        });
        std::set<int> received;
        start.count_down();start.wait();
        while(received.size()<4000) {
            auto value=queue.pop();
            if(value) {if(!received.insert(*value).second) throw std::runtime_error("duplicate handoff value");}
            else std::this_thread::yield();
        }
        for(auto& producer:producers) producer.join();
        if(queue.pop() || *received.begin()!=0 || *received.rbegin()!=3999) throw std::runtime_error("handoff reconciliation");
        std::cout<<"bounded handoff concurrent reconciliation passed\n";
    } catch(const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
