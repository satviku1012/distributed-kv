// Multi-threaded TCP storage node.
//
// Usage: ./node_server <port> <wal_file_path> [num_shards=16]

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <memory>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "protocol.h"
#include "sharded_store.h"

namespace {

std::atomic<bool> g_running{true};
std::atomic<int> g_listen_fd{-1};

void on_signal(int) {
    g_running.store(false);
    // close() is async-signal-safe; it unblocks accept() in the main loop.
    int fd = g_listen_fd.exchange(-1);
    if (fd >= 0) ::close(fd);
}

bool send_response(int fd, kv::Status st, const std::string* val = nullptr) {
    const uint32_t len = val ? static_cast<uint32_t>(val->size()) : 0;
    kv::ResponseHeader hdr = kv::make_response(st, len);
    if (!kv::write_exact(fd, &hdr, sizeof(hdr))) return false;
    return len == 0 || kv::write_exact(fd, val->data(), len);
}

void handle_connection(int fd, kv::ShardedKVStore& store) {
    kv::configure_socket(fd);
    std::string key, val, out;

    while (g_running.load(std::memory_order_relaxed)) {
        kv::RequestHeader hdr{};
        if (!kv::read_exact(fd, &hdr, sizeof(hdr))) break;
        if (hdr.magic != kv::kMagic || hdr.key_len > kv::kMaxKeyLen ||
            hdr.val_len > kv::kMaxValLen) {
            send_response(fd, kv::Status::ERROR);
            break;  // stream is out of sync; drop the connection
        }

        key.resize(hdr.key_len);
        val.resize(hdr.val_len);
        if (hdr.key_len && !kv::read_exact(fd, key.data(), hdr.key_len)) break;
        if (hdr.val_len && !kv::read_exact(fd, val.data(), hdr.val_len)) break;

        bool ok = true;
        switch (static_cast<kv::OpCode>(hdr.opcode)) {
            case kv::OpCode::PUT:
                ok = send_response(fd, store.put(key, val) ? kv::Status::OK : kv::Status::ERROR);
                break;
            case kv::OpCode::GET:
                ok = store.get(key, out) ? send_response(fd, kv::Status::OK, &out)
                                         : send_response(fd, kv::Status::NOT_FOUND);
                break;
            case kv::OpCode::DEL:
                ok = send_response(fd, store.del(key) ? kv::Status::OK : kv::Status::NOT_FOUND);
                break;
            case kv::OpCode::PING:
                ok = send_response(fd, kv::Status::OK);
                break;
            default:
                ok = send_response(fd, kv::Status::ERROR);
                break;
        }
        if (!ok) break;
    }
    ::close(fd);
}

int open_listener(int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd, SOMAXCONN) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr, "usage: %s <port> <wal_file_path> [num_shards=16]\n", argv[0]);
        return 1;
    }
    const int port = std::atoi(argv[1]);
    const std::string wal_path = argv[2];
    const size_t num_shards = argc > 3 ? std::strtoul(argv[3], nullptr, 10) : 16;

    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    // Connection threads are detached and may outlive main's loop, so the
    // store is intentionally never destroyed; it is flushed explicitly instead.
    kv::ShardedKVStore* store = nullptr;
    const auto t0 = std::chrono::steady_clock::now();
    try {
        store = new kv::ShardedKVStore(wal_path, num_shards);
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[node] %s\n", e.what());
        return 1;
    }
    const auto recover_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - t0)
                                .count();
    std::printf("[node] recovered %zu records (%zu keys) from %s in %.2f ms\n",
                store->recovered_records(), store->size(), wal_path.c_str(),
                static_cast<double>(recover_us) / 1000.0);

    int lfd = open_listener(port);
    if (lfd < 0) {
        std::fprintf(stderr, "[node] failed to listen on port %d: %s\n", port, std::strerror(errno));
        return 1;
    }
    g_listen_fd.store(lfd);
    std::printf("[node] listening on 0.0.0.0:%d with %zu shards\n", port, store->num_shards());
    std::fflush(stdout);

    while (g_running.load()) {
        int cfd = ::accept(lfd, nullptr, nullptr);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (!g_running.load()) break;
            std::perror("[node] accept");
            continue;
        }
        std::thread(handle_connection, cfd, std::ref(*store)).detach();
    }

    store->flush();
    std::printf("[node] shutting down, WAL flushed\n");
    std::fflush(stdout);
    std::_Exit(0);
}
