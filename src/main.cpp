#include "telemetry/outbox.hpp"
#include "telemetry/input.hpp"
#include <charconv>
#include <iostream>
#include <map>
#include <set>

namespace {
using telemetry::Json;
std::map<std::string, std::string> options(int argc, char **argv) {
    std::map<std::string, std::string> result;
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc || !std::string_view(argv[i]).starts_with("--") ||
            !result.emplace(argv[i] + 2, argv[i + 1]).second)
            throw std::invalid_argument("expected unique --name value options");
    }
    return result;
}
void validate_options(const std::string &command,
                      const std::map<std::string, std::string> &options) {
    std::set<std::string> allowed = {"db", "device", "max-items", "max-bytes",
                                     "max-quarantine", "max-pages"};
    if (command == "enqueue")
        allowed.insert({"count", "timestamp-ms"});
    else if (command == "quarantine")
        allowed.insert("limit");
    else if (command == "discard")
        allowed.insert({"stream", "sequence"});
    else if (command != "status" && command != "new-stream")
        throw std::invalid_argument("unknown command");
    for (const auto &[name, value] : options) {
        (void)value;
        if (!allowed.contains(name))
            throw std::invalid_argument("invalid option for " + command + ": --" + name);
    }
}
std::int64_t number(const std::map<std::string, std::string> &o, const std::string &key,
                    std::int64_t fallback) {
    const auto it = o.find(key);
    if (it == o.end())
        return fallback;
    std::int64_t n = 0;
    const auto [end, ec] =
        std::from_chars(it->second.data(), it->second.data() + it->second.size(), n);
    if (ec != std::errc{} || end != it->second.data() + it->second.size())
        throw std::invalid_argument("invalid numeric option");
    return n;
}
} // namespace
int main(int argc, char **argv) {
    try {
        if (argc < 2)
            throw std::invalid_argument(
                "usage: gateway enqueue|status|quarantine|new-stream|discard --db PATH --device ID "
                "[options]");
        const std::string command = argv[1];
        auto o = options(argc, argv);
        validate_options(command, o);

        std::optional<telemetry::Limits> limits;
        if (o.contains("max-items") || o.contains("max-bytes") || o.contains("max-quarantine") ||
            o.contains("max-pages"))
            limits =
                telemetry::Limits{number(o, "max-items", 4096), number(o, "max-bytes", 1048576),
                                  number(o, "max-quarantine", 64), number(o, "max-pages", 4096)};
        telemetry::Outbox outbox(o.at("db"), o.at("device"), limits);
        if (command == "enqueue") {
            const auto count = number(o, "count", 1);
            if (count < 1 || count > 10000)
                throw std::invalid_argument("count range 1..10000");
            const auto timestamp = number(o, "timestamp-ms", telemetry::wall_ms());
            telemetry::validate_timestamp_series(timestamp, count);
            bool rejected = false;
            for (std::int64_t i = 0; i < count; ++i) {
                auto r = outbox.enqueue(timestamp + i, 20000 + (i % 100), 101325 + (i % 17));
                Json j = {{"kind", r.accepted ? "accepted" : "rejected"}, {"reason", r.reason}};
                if (r.event)
                    j["event"] =
                        telemetry::parse_bounded(r.event->wire(), telemetry::max_event_bytes);
                std::cout << j.dump() << std::endl;
                rejected = rejected || !r.accepted;
            }
            return rejected ? 3 : 0;
        }
        if (command == "status")
            std::cout << outbox.status().dump() << '\n';
        else if (command == "quarantine")
            std::cout << outbox.terminal(static_cast<std::size_t>(number(o, "limit", 100))).dump()
                      << '\n';
        else if (command == "new-stream")
            std::cout << Json{{"stream_id", outbox.start_stream()}}.dump() << '\n';
        else if (command == "discard") {
            if (!outbox.discard_terminal(o.at("stream"), number(o, "sequence", 0)))
                return 4;
        } else
            throw std::invalid_argument("unknown command");
        outbox.checkpoint(true);
    } catch (const telemetry::StorageError &e) {
        std::cerr
            << Json{{"kind", "storage_error"}, {"sqlite_code", e.code}, {"detail", e.what()}}.dump()
            << '\n';
        return 2;
    } catch (const std::exception &e) {
        std::cerr << Json{{"kind", "error"}, {"detail", e.what()}}.dump() << '\n';
        return 2;
    }
}
