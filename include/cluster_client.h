#pragma once

// Smart cluster client: routes keys with the consistent hash ring, replicates
// writes, and fails reads over to replicas when a node is unreachable.
//
// A ClusterClient is not thread-safe; use one instance per thread so each
// thread owns its own pooled connections.

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "hash_ring.h"
#include "protocol.h"

namespace kv {

class ClusterClient {
public:
    explicit ClusterClient(const std::vector<std::string>& nodes, int timeout_ms = 200,
                           int vnodes = 32)
        : ring_(vnodes), timeout_ms_(timeout_ms) {
        for (const auto& n : nodes) ring_.add_node(n);
    }

    ~ClusterClient() {
        for (auto& [_, fd] : conns_) ::close(fd);
    }

    ClusterClient(const ClusterClient&) = delete;
    ClusterClient& operator=(const ClusterClient&) = delete;

    // Writes to every replica; succeeds if at least one acknowledges.
    bool put(const std::string& key, const std::string& val, size_t replicas = 2) {
        size_t acks = 0;
        for (const auto& node : ring_.get_preference_list(key, replicas)) {
            Status st;
            if (request(node, OpCode::PUT, key, val, st, nullptr) && st == Status::OK) ++acks;
        }
        return acks > 0;
    }

    // Reads from the primary, failing over to replicas on connection errors
    // or timeouts. A NOT_FOUND from a live node is authoritative.
    bool get(const std::string& key, std::string& val_out, size_t replicas = 2) {
        for (const auto& node : ring_.get_preference_list(key, replicas)) {
            Status st;
            if (!request(node, OpCode::GET, key, {}, st, &val_out)) continue;
            return st == Status::OK;
        }
        return false;
    }

    // Deletes from every replica; true if any replica held the key.
    bool del(const std::string& key, size_t replicas = 2) {
        bool found = false;
        for (const auto& node : ring_.get_preference_list(key, replicas)) {
            Status st;
            if (request(node, OpCode::DEL, key, {}, st, nullptr) && st == Status::OK) found = true;
        }
        return found;
    }

    // Reads a key from one specific node, bypassing routing and failover.
    bool get_from(const std::string& node, const std::string& key, std::string& val_out) {
        Status st;
        return request(node, OpCode::GET, key, {}, st, &val_out) && st == Status::OK;
    }

    bool ping(const std::string& node) {
        Status st;
        return request(node, OpCode::PING, {}, {}, st, nullptr) && st == Status::OK;
    }

    const ConsistentHashRing& ring() const { return ring_; }

private:
    // Returns false on a transport failure (the connection is then dropped
    // and re-established on the next call); `st` holds the server status.
    bool request(const std::string& node, OpCode op, std::string_view key, std::string_view val,
                 Status& st, std::string* val_out) {
        int fd = get_conn(node);
        if (fd < 0) return false;

        RequestHeader hdr = make_request(op, static_cast<uint32_t>(key.size()),
                                         static_cast<uint32_t>(val.size()));
        ResponseHeader resp{};
        bool ok = write_exact(fd, &hdr, sizeof(hdr)) &&
                  (key.empty() || write_exact(fd, key.data(), key.size())) &&
                  (val.empty() || write_exact(fd, val.data(), val.size())) &&
                  read_exact(fd, &resp, sizeof(resp)) && resp.magic == kMagic &&
                  resp.val_len <= kMaxValLen;

        if (ok) {
            if (val_out) {
                val_out->resize(resp.val_len);
                ok = resp.val_len == 0 || read_exact(fd, val_out->data(), resp.val_len);
            } else if (resp.val_len > 0) {
                std::string discard(resp.val_len, '\0');
                ok = read_exact(fd, discard.data(), resp.val_len);
            }
        }
        if (!ok) {
            drop_conn(node);
            return false;
        }
        st = static_cast<Status>(resp.status);
        return true;
    }

    int get_conn(const std::string& node) {
        auto it = conns_.find(node);
        if (it != conns_.end()) return it->second;
        int fd = connect_to(node);
        if (fd >= 0) conns_.emplace(node, fd);
        return fd;
    }

    void drop_conn(const std::string& node) {
        auto it = conns_.find(node);
        if (it == conns_.end()) return;
        ::close(it->second);
        conns_.erase(it);
    }

    // Non-blocking connect bounded by timeout_ms_, so a dead host can't stall
    // the caller for the OS default connect timeout.
    int connect_to(const std::string& node) const {
        auto colon = node.rfind(':');
        if (colon == std::string::npos) return -1;
        const std::string host = node.substr(0, colon);
        const int port = std::atoi(node.c_str() + colon + 1);

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
            addrinfo hints{}, *res = nullptr;
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            if (::getaddrinfo(host.c_str(), nullptr, &hints, &res) != 0 || !res) return -1;
            addr.sin_addr = reinterpret_cast<sockaddr_in*>(res->ai_addr)->sin_addr;
            ::freeaddrinfo(res);
        }

        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;
        const int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (rc != 0 && errno == EINPROGRESS) {
            pollfd pfd{fd, POLLOUT, 0};
            int err = 0;
            socklen_t len = sizeof(err);
            if (::poll(&pfd, 1, timeout_ms_) == 1 &&
                ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) {
                rc = 0;
            }
        }
        if (rc != 0) {
            ::close(fd);
            return -1;
        }

        ::fcntl(fd, F_SETFL, flags);
        configure_socket(fd);
        set_socket_timeout(fd, timeout_ms_);
        return fd;
    }

    ConsistentHashRing ring_;
    int timeout_ms_;
    std::unordered_map<std::string, int> conns_;
};

}  // namespace kv
