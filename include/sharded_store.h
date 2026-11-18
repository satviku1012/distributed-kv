#pragma once

// Sharded in-memory key-value store backed by an append-only binary WAL.
//
// WAL record layout (host byte order):
//   [Checksum: 4B] [OpType: 1B] [KeyLen: 4B] [ValLen: 4B] [Key] [Value]
// The checksum is FNV-1a over everything after the checksum field.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "hash_ring.h"

namespace kv {

enum class WalOp : uint8_t { PUT = 1, DEL = 3 };

class ShardedKVStore {
public:
    static constexpr size_t kWalHeaderSize = 4 + 1 + 4 + 4;
    static constexpr uint32_t kMaxWalKeyLen = 64 * 1024;
    static constexpr uint32_t kMaxWalValLen = 64 * 1024 * 1024;

    // fsync_each_write trades throughput for durability against power loss;
    // without it, records are handed to the OS page cache on every write and
    // survive process crashes (kill -9) but not a machine crash.
    explicit ShardedKVStore(const std::string& wal_path, size_t num_shards = 16,
                            bool fsync_each_write = false)
        : num_shards_(num_shards == 0 ? 1 : num_shards),
          shards_(std::make_unique<Shard[]>(num_shards_)),
          wal_path_(wal_path),
          fsync_each_write_(fsync_each_write) {
        recovered_ = recover(wal_path_);
        wal_fd_ = ::open(wal_path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
        if (wal_fd_ < 0) {
            throw std::runtime_error("failed to open WAL " + wal_path_ + ": " +
                                     std::strerror(errno));
        }
    }

    ~ShardedKVStore() {
        if (wal_fd_ >= 0) {
            ::fsync(wal_fd_);
            ::close(wal_fd_);
        }
    }

    ShardedKVStore(const ShardedKVStore&) = delete;
    ShardedKVStore& operator=(const ShardedKVStore&) = delete;

    // Returns false if the WAL append failed; memory is left unchanged.
    bool put(const std::string& key, const std::string& val) {
        Shard& s = shard_for(key);
        // Lock order is always shard -> WAL. Holding the shard lock across the
        // append keeps WAL order and in-memory order identical for each key.
        std::unique_lock lk(s.mtx);
        if (!append_wal(WalOp::PUT, key, val)) return false;
        s.map[key] = val;
        return true;
    }

    bool get(const std::string& key, std::string& val_out) const {
        const Shard& s = shard_for(key);
        std::shared_lock lk(s.mtx);
        auto it = s.map.find(key);
        if (it == s.map.end()) return false;
        val_out = it->second;
        return true;
    }

    // Returns true if the key existed and was removed.
    bool del(const std::string& key) {
        Shard& s = shard_for(key);
        std::unique_lock lk(s.mtx);
        auto it = s.map.find(key);
        if (it == s.map.end()) return false;
        if (!append_wal(WalOp::DEL, key, {})) return false;
        s.map.erase(it);
        return true;
    }

    // Forces buffered WAL data to stable storage (used on graceful shutdown).
    bool flush() {
        std::lock_guard lk(wal_mtx_);
        return wal_fd_ >= 0 && ::fsync(wal_fd_) == 0;
    }

    // Replays the WAL into memory. A truncated or corrupt record ends replay
    // and the file is truncated back to the last valid record.
    size_t recover(const std::string& wal_path) {
        std::vector<char> buf;
        if (!read_file(wal_path, buf)) return 0;

        size_t off = 0;
        size_t replayed = 0;
        const char* reason = nullptr;

        while (off < buf.size()) {
            if (buf.size() - off < kWalHeaderSize) {
                reason = "truncated header";
                break;
            }
            const char* p = buf.data() + off;
            uint32_t checksum, key_len, val_len;
            uint8_t op;
            std::memcpy(&checksum, p, 4);
            std::memcpy(&op, p + 4, 1);
            std::memcpy(&key_len, p + 5, 4);
            std::memcpy(&val_len, p + 9, 4);

            if ((op != static_cast<uint8_t>(WalOp::PUT) && op != static_cast<uint8_t>(WalOp::DEL)) ||
                key_len > kMaxWalKeyLen || val_len > kMaxWalValLen) {
                reason = "invalid header";
                break;
            }
            size_t frame_len = kWalHeaderSize + key_len + val_len;
            if (buf.size() - off < frame_len) {
                reason = "truncated payload";
                break;
            }
            if (fnv1a_hash(std::string_view(p + 4, frame_len - 4)) != checksum) {
                reason = "checksum mismatch";
                break;
            }

            std::string key(p + kWalHeaderSize, key_len);
            Shard& s = shard_for(key);
            std::unique_lock lk(s.mtx);
            if (op == static_cast<uint8_t>(WalOp::PUT)) {
                s.map[std::move(key)].assign(p + kWalHeaderSize + key_len, val_len);
            } else {
                s.map.erase(key);
            }
            off += frame_len;
            ++replayed;
        }

        if (reason != nullptr) {
            std::fprintf(stderr,
                         "[wal] %s at offset %zu of %zu; truncating torn tail\n",
                         reason, off, buf.size());
            if (::truncate(wal_path.c_str(), static_cast<off_t>(off)) != 0) {
                std::fprintf(stderr, "[wal] truncate failed: %s\n", std::strerror(errno));
            }
        }
        return replayed;
    }

    size_t recovered_records() const { return recovered_; }
    size_t num_shards() const { return num_shards_; }

    size_t size() const {
        size_t n = 0;
        for (size_t i = 0; i < num_shards_; ++i) {
            std::shared_lock lk(shards_[i].mtx);
            n += shards_[i].map.size();
        }
        return n;
    }

private:
    struct alignas(64) Shard {
        mutable std::shared_mutex mtx;
        std::unordered_map<std::string, std::string> map;
    };

    Shard& shard_for(std::string_view key) { return shards_[fnv1a_hash(key) % num_shards_]; }
    const Shard& shard_for(std::string_view key) const {
        return shards_[fnv1a_hash(key) % num_shards_];
    }

    bool append_wal(WalOp op, std::string_view key, std::string_view val) {
        const auto key_len = static_cast<uint32_t>(key.size());
        const auto val_len = static_cast<uint32_t>(val.size());

        // Built with append rather than memcpy into a pre-sized buffer: GCC 15
        // misreads the size math after inlining and raises a false -Warray-bounds.
        std::string frame;
        frame.reserve(kWalHeaderSize + key.size() + val.size());
        frame.append(4, '\0');  // checksum, filled in below
        frame.push_back(static_cast<char>(op));
        frame.append(reinterpret_cast<const char*>(&key_len), 4);
        frame.append(reinterpret_cast<const char*>(&val_len), 4);
        frame.append(key);
        frame.append(val);
        const uint32_t checksum = fnv1a_hash(std::string_view(frame).substr(4));
        std::memcpy(frame.data(), &checksum, 4);

        std::lock_guard lk(wal_mtx_);
        size_t written = 0;
        while (written < frame.size()) {
            ssize_t n = ::write(wal_fd_, frame.data() + written, frame.size() - written);
            if (n < 0) {
                if (errno == EINTR) continue;
                return false;
            }
            written += static_cast<size_t>(n);
        }
        if (fsync_each_write_ && ::fsync(wal_fd_) != 0) return false;
        return true;
    }

    static bool read_file(const std::string& path, std::vector<char>& out) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) return false;  // no WAL yet: fresh node
        struct stat st {};
        if (::fstat(fd, &st) != 0) {
            ::close(fd);
            return false;
        }
        out.resize(static_cast<size_t>(st.st_size));
        size_t got = 0;
        while (got < out.size()) {
            ssize_t n = ::read(fd, out.data() + got, out.size() - got);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) break;
            got += static_cast<size_t>(n);
        }
        out.resize(got);
        ::close(fd);
        return true;
    }

    size_t num_shards_;
    std::unique_ptr<Shard[]> shards_;
    std::string wal_path_;
    bool fsync_each_write_;
    std::mutex wal_mtx_;
    int wal_fd_ = -1;
    size_t recovered_ = 0;
};

}  // namespace kv
