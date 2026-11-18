// Unit tests for the hash ring and the sharded store / WAL.
// Dependency-free: each test is a function, failures are counted and reported.

#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "hash_ring.h"
#include "sharded_store.h"

namespace {

int g_failures = 0;

#define CHECK(cond)                                                              \
    do {                                                                         \
        if (!(cond)) {                                                           \
            std::fprintf(stderr, "    FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                        \
        }                                                                        \
    } while (0)

std::string temp_wal() {
    const char* dir = std::getenv("TMPDIR");
    std::string path = std::string(dir ? dir : "/tmp") + "/kv-unit-XXXXXX";
    int fd = ::mkstemp(path.data());
    if (fd >= 0) ::close(fd);
    ::unlink(path.c_str());  // the store creates it
    return path;
}

off_t file_size(const std::string& path) {
    struct stat st {};
    return ::stat(path.c_str(), &st) == 0 ? st.st_size : -1;
}

void append_raw(const std::string& path, const void* data, size_t len) {
    int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    if (fd < 0) return;
    if (::write(fd, data, len) < 0) std::perror("write");
    ::close(fd);
}

// ---- hash ring ------------------------------------------------------------

void test_fnv1a_known_vectors() {
    CHECK(kv::fnv1a_hash("") == 0x811c9dc5u);
    CHECK(kv::fnv1a_hash("a") == 0xe40c292cu);
    CHECK(kv::fnv1a_hash("foobar") == 0xbf9cf968u);
}

void test_preference_list_is_distinct_and_capped() {
    kv::ConsistentHashRing ring;
    for (const char* n : {"a:1", "b:2", "c:3"}) ring.add_node(n);
    CHECK(ring.size() == 3 * 32);

    for (int i = 0; i < 1000; ++i) {
        auto pref = ring.get_preference_list("key-" + std::to_string(i), 2);
        CHECK(pref.size() == 2);
        CHECK(pref[0] != pref[1]);
    }
    // Asking for more replicas than nodes returns every node once.
    auto all = ring.get_preference_list("x", 10);
    CHECK(all.size() == 3);
    CHECK(std::set<std::string>(all.begin(), all.end()).size() == 3);
}

void test_empty_ring() {
    kv::ConsistentHashRing ring;
    CHECK(ring.get_preference_list("x", 2).empty());
}

void test_remove_node_only_moves_its_keys() {
    kv::ConsistentHashRing ring;
    for (const char* n : {"a:1", "b:2", "c:3"}) ring.add_node(n);

    std::map<std::string, std::string> before;
    for (int i = 0; i < 2000; ++i) {
        std::string k = "key-" + std::to_string(i);
        before[k] = ring.get_preference_list(k, 1)[0];
    }
    ring.remove_node("b:2");
    CHECK(ring.size() == 2 * 32);

    for (const auto& [k, owner] : before) {
        const std::string now = ring.get_preference_list(k, 1)[0];
        CHECK(now != "b:2");
        if (owner != "b:2") CHECK(now == owner);  // consistent hashing: no reshuffle
    }
}

void test_distribution_is_reasonably_even() {
    kv::ConsistentHashRing ring;
    for (const char* n : {"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"}) ring.add_node(n);
    std::map<std::string, int> counts;
    const int keys = 30000;
    for (int i = 0; i < keys; ++i) ++counts[ring.get_preference_list("k" + std::to_string(i), 1)[0]];
    for (const auto& [_, c] : counts) {
        CHECK(c > keys / 5);      // every node owns more than 20%
        CHECK(c < keys / 2);      // and less than 50%
    }
}

// ---- sharded store / WAL --------------------------------------------------

void test_put_get_del() {
    const std::string wal = temp_wal();
    kv::ShardedKVStore s(wal);
    std::string v;
    CHECK(!s.get("k", v));
    CHECK(s.put("k", "v1"));
    CHECK(s.get("k", v) && v == "v1");
    CHECK(s.put("k", "v2"));
    CHECK(s.get("k", v) && v == "v2");
    CHECK(s.del("k"));
    CHECK(!s.del("k"));
    CHECK(!s.get("k", v));
    CHECK(s.put("empty", ""));
    CHECK(s.get("empty", v) && v.empty());
    ::unlink(wal.c_str());
}

void test_recovery_replays_puts_and_deletes() {
    const std::string wal = temp_wal();
    {
        kv::ShardedKVStore s(wal);
        for (int i = 0; i < 100; ++i) s.put("k" + std::to_string(i), "v" + std::to_string(i));
        s.put("k5", "overwritten");
        s.del("k7");
    }
    kv::ShardedKVStore s(wal);
    CHECK(s.recovered_records() == 102);
    CHECK(s.size() == 99);
    std::string v;
    CHECK(s.get("k5", v) && v == "overwritten");
    CHECK(!s.get("k7", v));
    CHECK(s.get("k99", v) && v == "v99");
    ::unlink(wal.c_str());
}

void test_torn_tail_is_truncated() {
    const std::string wal = temp_wal();
    { kv::ShardedKVStore s(wal); s.put("a", "1"); s.put("b", "2"); }
    const off_t good = file_size(wal);

    const char partial[] = {0x01, 0x02, 0x03, 0x04, 0x01, 0x05, 0x00};  // half a header
    append_raw(wal, partial, sizeof(partial));
    CHECK(file_size(wal) == good + static_cast<off_t>(sizeof(partial)));

    { kv::ShardedKVStore s(wal); CHECK(s.size() == 2); }
    CHECK(file_size(wal) == good);
    ::unlink(wal.c_str());
}

void test_corrupt_record_stops_replay() {
    const std::string wal = temp_wal();
    { kv::ShardedKVStore s(wal); s.put("a", "1"); }
    const off_t first = file_size(wal);
    { kv::ShardedKVStore s(wal); s.put("b", "2"); }

    // Flip one byte inside the second record's value.
    int fd = ::open(wal.c_str(), O_RDWR);
    char c = 0;
    const off_t pos = file_size(wal) - 1;
    CHECK(::pread(fd, &c, 1, pos) == 1);
    c ^= 0xFF;
    CHECK(::pwrite(fd, &c, 1, pos) == 1);
    ::close(fd);

    kv::ShardedKVStore s(wal);
    std::string v;
    CHECK(s.get("a", v) && v == "1");
    CHECK(!s.get("b", v));
    CHECK(file_size(wal) == first);
    ::unlink(wal.c_str());
}

void test_concurrent_writers() {
    const std::string wal = temp_wal();
    {
        kv::ShardedKVStore s(wal);
        std::vector<std::thread> threads;
        for (int t = 0; t < 8; ++t) {
            threads.emplace_back([&s, t] {
                for (int i = 0; i < 1000; ++i) {
                    s.put("k" + std::to_string(t * 1000 + i), std::to_string(i));
                    s.put("shared", std::to_string(t));  // contended key
                }
            });
        }
        for (auto& th : threads) th.join();
        CHECK(s.size() == 8001);
    }
    // The last write to "shared" in memory must match what the WAL replays.
    std::string live;
    { kv::ShardedKVStore s(wal); CHECK(s.size() == 8001); s.get("shared", live); }
    kv::ShardedKVStore again(wal);
    std::string replayed;
    CHECK(again.get("shared", replayed) && replayed == live);
    ::unlink(wal.c_str());
}

void test_single_shard_mode() {
    const std::string wal = temp_wal();
    kv::ShardedKVStore s(wal, 1);
    CHECK(s.num_shards() == 1);
    for (int i = 0; i < 100; ++i) s.put("k" + std::to_string(i), "v");
    CHECK(s.size() == 100);
    ::unlink(wal.c_str());
}

}  // namespace

int main() {
    const std::vector<std::pair<const char*, std::function<void()>>> tests = {
        {"fnv1a known vectors", test_fnv1a_known_vectors},
        {"preference list is distinct and capped", test_preference_list_is_distinct_and_capped},
        {"empty ring", test_empty_ring},
        {"remove node only moves its keys", test_remove_node_only_moves_its_keys},
        {"distribution is reasonably even", test_distribution_is_reasonably_even},
        {"put / get / del", test_put_get_del},
        {"recovery replays puts and deletes", test_recovery_replays_puts_and_deletes},
        {"torn tail is truncated", test_torn_tail_is_truncated},
        {"corrupt record stops replay", test_corrupt_record_stops_replay},
        {"concurrent writers", test_concurrent_writers},
        {"single shard mode", test_single_shard_mode},
    };

    for (const auto& [name, fn] : tests) {
        const int before = g_failures;
        fn();
        std::printf("%s %s\n", g_failures == before ? "[pass]" : "[FAIL]", name);
    }
    std::printf("\n%zu tests, %d failed checks\n", tests.size(), g_failures);
    return g_failures == 0 ? 0 : 1;
}
