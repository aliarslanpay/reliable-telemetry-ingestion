#include "telemetry/contract.hpp"
#include <fstream>
#include <iostream>
#include <stdexcept>

void check(bool value) { if (!value) throw std::runtime_error("check failed"); }
template<class F> void rejects(F f) {
    bool failed = false;
    try { f(); } catch (const std::exception&) { failed = true; }
    check(failed);
}
int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::runtime_error("fixtures required");
        std::ifstream file(argv[1]);
        auto fixtures = telemetry::Json::parse(file);
        for (const auto& fixture : fixtures) {
            const auto e = telemetry::parse_event(fixture.at("event").dump());
            check(e.canonical() == fixture.at("canonical").get<std::string>());
            check(e.fingerprint == fixture.at("sha256").get<std::string>());
            auto ack = telemetry::Json{{"schema_version",1},{"device_id",e.device_id},{"stream_id",e.stream_id},
                {"sequence",e.sequence},{"fingerprint",e.fingerprint},{"result","stored"}};
            check(telemetry::parse_ack(ack.dump()).fingerprint == e.fingerprint);
            auto bad = fixture.at("event"); bad["sequence"] = true;
            rejects([&] { telemetry::parse_event(bad.dump()); });
            bad = fixture.at("event"); bad["pressure_pa"] = 1.5;
            rejects([&] { telemetry::parse_event(bad.dump()); });
            bad = fixture.at("event"); bad["temperature_mc"] = 200001;
            rejects([&] { telemetry::parse_event(bad.dump()); });
        }
        rejects([] { telemetry::parse_bounded("{\"a\":1,\"a\":2}", 100); });
        rejects([] { telemetry::parse_bounded("[[[[[[0]]]]]]", 100); });
        rejects([] { telemetry::parse_event(std::string(1025, 'x')); });
        check(telemetry::lower_hex(telemetry::new_stream_id(), 32));
        std::cout << "contract fixtures and rejection guards passed\n";
    } catch (const std::exception& e) { std::cerr << e.what() << '\n'; return 1; }
}
