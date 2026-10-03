#include "telemetry/outbox.hpp"
#include "telemetry/input.hpp"
#include "telemetry/service.hpp"
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <map>
#include <set>
#include <sys/file.h>
#include <unistd.h>

namespace {
using telemetry::Json;
volatile std::sig_atomic_t stopping = 0;
void stop(int) {
    stopping = 1;
}
std::string environment(const char *name) {
    const auto *v = std::getenv(name);
    return v ? v : "";
}
struct Lock {
    int fd = -1;
    explicit Lock(const std::string &path) {
        fd = ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC, 0600);
        if (fd < 0 || flock(fd, LOCK_EX | LOCK_NB) != 0) {
            if (fd >= 0)
                close(fd);
            throw std::runtime_error("another gateway owns this outbox, or lock unavailable");
        }
    }
    ~Lock() {
        if (fd >= 0)
            close(fd);
    }
};
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
    else if (command == "run")
        allowed.insert({"host", "port", "count", "timestamp-ms", "duration-ms",
                        "ack-timeout-ms", "interval-ms", "window", "stay", "replay"});
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
std::int64_t bounded_number(const std::map<std::string, std::string> &options,
                            const std::string &key, std::int64_t fallback,
                            std::int64_t low, std::int64_t high) {
    const auto value = number(options, key, fallback);
    if (value < low || value > high)
        throw std::invalid_argument(key + " range " + std::to_string(low) + ".." +
                                    std::to_string(high));
    return value;
}
} // namespace
int main(int argc, char **argv) {
    try {
        if (argc < 2)
            throw std::invalid_argument(
                "usage: gateway run|enqueue|status|quarantine|new-stream|discard --db PATH "
                "--device ID [options]");
        const std::string command = argv[1];
        auto o = options(argc, argv);
        validate_options(command, o);

        std::optional<telemetry::Limits> limits;
        if (o.contains("max-items") || o.contains("max-bytes") || o.contains("max-quarantine") ||
            o.contains("max-pages"))
            limits =
                telemetry::Limits{number(o, "max-items", 4096), number(o, "max-bytes", 1048576),
                                  number(o, "max-quarantine", 64), number(o, "max-pages", 4096)};
        if (command != "enqueue" && command != "run" && !std::filesystem::exists(o.at("db")))
            throw std::invalid_argument("outbox does not exist");
        telemetry::Outbox outbox(o.at("db"), o.at("device"), limits);
        if (command == "run") {
            Lock lock(o.at("db") + ".lock");
            telemetry::RunConfig c;
            c.mqtt.device = o.at("device");
            c.mqtt.username = environment("MQTT_USERNAME");
            c.mqtt.password = environment("MQTT_PASSWORD");
            if (o.contains("host"))
                c.mqtt.host = o.at("host");
            c.mqtt.port = static_cast<int>(bounded_number(o, "port", 1883, 1, 65535));
            c.mqtt.ca_file = environment("MQTT_CA_FILE");
            c.mqtt.cert_file = environment("MQTT_CERT_FILE");
            c.mqtt.key_file = environment("MQTT_KEY_FILE");
            c.count = number(o, "count", 0);
            c.timestamp_ms = number(o, "timestamp-ms", telemetry::wall_ms());
            c.duration_ms = number(o, "duration-ms", 30000);
            c.ack_timeout_ms = number(o, "ack-timeout-ms", 1000);
            c.interval_ms = number(o, "interval-ms", 0);
            c.window = static_cast<unsigned>(bounded_number(o, "window", 8, 1, 64));
            c.stay = number(o, "stay", 0) != 0;
            if (o.contains("replay"))
                c.replay = o.at("replay");
            std::signal(SIGINT, stop);
            std::signal(SIGTERM, stop);
            return telemetry::run_service(outbox, c, stopping);
        }
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
