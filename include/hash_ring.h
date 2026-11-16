#pragma once

// Consistent hash ring with virtual nodes.
//
// The ring is not internally synchronized: build it once (add_node/remove_node)
// and then share it read-only across threads, or guard mutations externally.

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace kv {

// 32-bit FNV-1a.
inline uint32_t fnv1a_hash(std::string_view data) {
    uint32_t h = 2166136261u;
    for (unsigned char c : data) {
        h ^= c;
        h *= 16777619u;
    }
    return h;
}

// Ring position: FNV-1a followed by the MurmurHash3 32-bit finalizer.
// Plain FNV-1a barely changes its high bits when only the last characters
// differ ("host:8001#v0" vs "host:8001#v1"), which clusters virtual nodes on
// the ring. The finalizer spreads every input bit across the whole output.
inline uint32_t ring_hash(std::string_view data) {
    uint32_t h = fnv1a_hash(data);
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int vnodes = 32) : vnodes_(vnodes > 0 ? vnodes : 1) {}

    void add_node(const std::string& host_port) {
        for (int i = 0; i < vnodes_; ++i) {
            ring_[ring_hash(vnode_key(host_port, i))] = host_port;
        }
    }

    void remove_node(const std::string& host_port) {
        for (auto it = ring_.begin(); it != ring_.end();) {
            if (it->second == host_port) {
                it = ring_.erase(it);
            } else {
                ++it;
            }
        }
    }

    // Returns up to `replication_factor` distinct physical nodes, walking
    // clockwise from the key's position. Index 0 is the primary.
    std::vector<std::string> get_preference_list(std::string_view key,
                                                 size_t replication_factor = 2) const {
        std::vector<std::string> out;
        if (ring_.empty() || replication_factor == 0) return out;

        out.reserve(replication_factor);
        std::unordered_set<std::string_view> seen;

        auto it = ring_.lower_bound(ring_hash(key));
        for (size_t steps = 0; steps < ring_.size() && out.size() < replication_factor; ++steps) {
            if (it == ring_.end()) it = ring_.begin();
            if (seen.insert(it->second).second) out.push_back(it->second);
            ++it;
        }
        return out;
    }

    size_t size() const { return ring_.size(); }
    bool empty() const { return ring_.empty(); }
    int vnodes() const { return vnodes_; }

private:
    static std::string vnode_key(const std::string& host_port, int i) {
        return host_port + "#v" + std::to_string(i);
    }

    int vnodes_;
    std::map<uint32_t, std::string> ring_;
};

}  // namespace kv
