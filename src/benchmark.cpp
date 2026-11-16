// Load generator and resilience checks.
//
//   ./benchmark --mode=throughput --nodes=H:P,H:P,... [--threads=16] [--ops=100000]
//               [--keys=10000] [--put-ratio=0.2] [--replicas=2] [--value-size=64]
//   ./benchmark --mode=failover-write --nodes=... [--keys=5000]
//   ./benchmark --mode=failover-read  --nodes=... [--keys=5000]
//   ./benchmark --mode=recovery --nodes=... --target=H:P [--keys=5000]
//
// failover-write populates replicated keys; failover-read reads them back
// through the client (exercising replica failover) and exits non-zero unless
// every key is found. recovery reads directly from --target every failover
// key whose preference list includes that node, verifying WAL recovery.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cluster_client.h"

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::string mode = "throughput";
    std::vector<std::string> nodes;
    std::string target;
    size_t threads = 16;
    size_t ops = 100000;
    size_t keys = 0;  // 0 = per-mode default
    double put_ratio = 0.2;
    size_t replicas = 2;
    size_t value_size = 64;
};

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, sep)) {
        if (!item.empty()) out.push_back(item);
    }
    return out;
}

bool parse_args(int argc, char** argv, Options& o) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        auto eq = a.find('=');
        if (a.rfind("--", 0) != 0 || eq == std::string::npos) {
            std::fprintf(stderr, "bad argument: %s\n", a.c_str());
            return false;
        }
        std::string k = a.substr(2, eq - 2), v = a.substr(eq + 1);
        if (k == "mode") o.mode = v;
        else if (k == "nodes") o.nodes = split(v, ',');
        else if (k == "target") o.target = v;
        else if (k == "threads") o.threads = std::strtoul(v.c_str(), nullptr, 10);
        else if (k == "ops") o.ops = std::strtoul(v.c_str(), nullptr, 10);
        else if (k == "keys") o.keys = std::strtoul(v.c_str(), nullptr, 10);
        else if (k == "put-ratio") o.put_ratio = std::strtod(v.c_str(), nullptr);
        else if (k == "replicas") o.replicas = std::strtoul(v.c_str(), nullptr, 10);
        else if (k == "value-size") o.value_size = std::strtoul(v.c_str(), nullptr, 10);
        else {
            std::fprintf(stderr, "unknown option: --%s\n", k.c_str());
            return false;
        }
    }
    if (o.nodes.empty()) {
        std::fprintf(stderr, "--nodes is required\n");
        return false;
    }
    if (o.threads == 0) o.threads = 1;
    return true;
}

std::string bench_key(size_t i) { return "bench-key-" + std::to_string(i); }
std::string failover_key(size_t i) { return "fo-key-" + std::to_string(i); }
std::string failover_val(size_t i) { return "fo-val-" + std::to_string(i); }

double percentile_us(const std::vector<uint64_t>& sorted_ns, double p) {
    if (sorted_ns.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p * static_cast<double>(sorted_ns.size() - 1));
    return static_cast<double>(sorted_ns[idx]) / 1000.0;
}

void print_distribution(const kv::ConsistentHashRing& ring, size_t keys,
                        const std::vector<std::string>& nodes) {
    std::map<std::string, size_t> counts;
    for (const auto& n : nodes) counts[n] = 0;
    for (size_t i = 0; i < keys; ++i) {
        auto pref = ring.get_preference_list(bench_key(i), 1);
        if (!pref.empty()) ++counts[pref[0]];
    }
    std::printf("primary key distribution (%zu keys, %d vnodes/node):\n", keys, ring.vnodes());
    for (const auto& [node, c] : counts) {
        std::printf("  %-22s %6zu  (%5.1f%%)\n", node.c_str(), c,
                    100.0 * static_cast<double>(c) / static_cast<double>(keys));
    }
}

int run_throughput(const Options& o) {
    const size_t keys = o.keys ? o.keys : 10000;
    const std::string value(o.value_size, 'x');

    // Pre-populate so GETs hit real data instead of measuring misses.
    {
        std::vector<std::thread> loaders;
        std::atomic<size_t> failed{0};
        for (size_t t = 0; t < o.threads; ++t) {
            loaders.emplace_back([&, t] {
                kv::ClusterClient c(o.nodes);
                for (size_t i = t; i < keys; i += o.threads) {
                    if (!c.put(bench_key(i), value, o.replicas)) ++failed;
                }
            });
        }
        for (auto& th : loaders) th.join();
        if (failed > 0) {
            std::fprintf(stderr, "pre-population failed for %zu keys; is the cluster up?\n",
                         failed.load());
            return 1;
        }
    }

    std::vector<std::vector<uint64_t>> lat(o.threads);
    std::atomic<size_t> errors{0}, puts{0}, gets{0};
    const size_t per_thread = o.ops / o.threads;

    const auto start = Clock::now();
    {
        std::vector<std::thread> workers;
        for (size_t t = 0; t < o.threads; ++t) {
            workers.emplace_back([&, t] {
                kv::ClusterClient c(o.nodes);
                std::mt19937_64 rng(0x9E3779B97F4A7C15ULL ^ t);
                std::uniform_int_distribution<size_t> key_dist(0, keys - 1);
                std::uniform_real_distribution<double> op_dist(0.0, 1.0);
                std::string out;
                auto& my = lat[t];
                my.reserve(per_thread);
                size_t local_puts = 0, local_gets = 0, local_err = 0;

                for (size_t i = 0; i < per_thread; ++i) {
                    const std::string k = bench_key(key_dist(rng));
                    const bool is_put = op_dist(rng) < o.put_ratio;
                    const auto t0 = Clock::now();
                    const bool ok = is_put ? c.put(k, value, o.replicas)
                                           : c.get(k, out, o.replicas);
                    my.push_back(static_cast<uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0)
                            .count()));
                    is_put ? ++local_puts : ++local_gets;
                    if (!ok) ++local_err;
                }
                puts += local_puts;
                gets += local_gets;
                errors += local_err;
            });
        }
        for (auto& th : workers) th.join();
    }
    const double secs = std::chrono::duration<double>(Clock::now() - start).count();

    std::vector<uint64_t> all;
    for (auto& v : lat) all.insert(all.end(), v.begin(), v.end());
    std::sort(all.begin(), all.end());

    std::printf("=== throughput: %zu nodes, %zu threads, %zu ops (%.0f%% PUT, R=%zu) ===\n",
                o.nodes.size(), o.threads, all.size(), o.put_ratio * 100.0, o.replicas);
    std::printf("elapsed:     %.3f s\n", secs);
    std::printf("throughput:  %.0f ops/sec\n", static_cast<double>(all.size()) / secs);
    std::printf("latency:     p50 %.1f us | p95 %.1f us | p99 %.1f us | max %.1f us\n",
                percentile_us(all, 0.50), percentile_us(all, 0.95), percentile_us(all, 0.99),
                percentile_us(all, 1.0));
    std::printf("ops:         %zu PUT, %zu GET, %zu errors\n", puts.load(), gets.load(),
                errors.load());

    kv::ClusterClient c(o.nodes);
    print_distribution(c.ring(), keys, o.nodes);
    return errors == 0 ? 0 : 1;
}

int run_failover_write(const Options& o) {
    const size_t keys = o.keys ? o.keys : 5000;
    kv::ClusterClient c(o.nodes);
    size_t ok = 0;
    for (size_t i = 0; i < keys; ++i) {
        if (c.put(failover_key(i), failover_val(i), o.replicas)) ++ok;
    }
    std::printf("=== failover-write: %zu/%zu keys written with R=%zu ===\n", ok, keys, o.replicas);
    return ok == keys ? 0 : 1;
}

int run_failover_read(const Options& o) {
    const size_t keys = o.keys ? o.keys : 5000;
    kv::ClusterClient c(o.nodes);
    size_t hits = 0, wrong = 0;
    std::string out;
    const auto start = Clock::now();
    for (size_t i = 0; i < keys; ++i) {
        if (c.get(failover_key(i), out, o.replicas)) {
            out == failover_val(i) ? ++hits : ++wrong;
        }
    }
    const double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
    std::printf("=== failover-read: %zu/%zu keys read back (%.2f%% hit rate, %zu wrong) in %.1f ms ===\n",
                hits, keys, 100.0 * static_cast<double>(hits) / static_cast<double>(keys), wrong, ms);
    return hits == keys ? 0 : 1;
}

int run_recovery(const Options& o) {
    if (o.target.empty()) {
        std::fprintf(stderr, "--target is required for recovery mode\n");
        return 1;
    }
    const size_t keys = o.keys ? o.keys : 5000;
    kv::ClusterClient c(o.nodes);
    size_t expected = 0, found = 0;
    std::string out;
    for (size_t i = 0; i < keys; ++i) {
        auto pref = c.ring().get_preference_list(failover_key(i), o.replicas);
        if (std::find(pref.begin(), pref.end(), o.target) == pref.end()) continue;
        ++expected;
        if (c.get_from(o.target, failover_key(i), out) && out == failover_val(i)) ++found;
    }
    std::printf("=== recovery: %s holds %zu/%zu of its replicated keys ===\n", o.target.c_str(),
                found, expected);
    return expected > 0 && found == expected ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    Options o;
    if (!parse_args(argc, argv, o)) return 2;

    if (o.mode == "throughput") return run_throughput(o);
    if (o.mode == "failover-write") return run_failover_write(o);
    if (o.mode == "failover-read") return run_failover_read(o);
    if (o.mode == "recovery") return run_recovery(o);

    std::fprintf(stderr, "unknown mode: %s\n", o.mode.c_str());
    return 2;
}
