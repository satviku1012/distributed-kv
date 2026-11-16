#pragma once

// Binary TCP wire protocol and POSIX socket helpers.
//
// All multi-byte header fields are sent in host byte order. The cluster is
// expected to run on little-endian hosts (x86-64 / ARM64), which keeps the
// hot path free of byte swapping.

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

namespace kv {

inline constexpr uint32_t kMagic = 0x4B565354;  // "KVST"

// Upper bounds to reject garbage headers before allocating buffers.
inline constexpr uint32_t kMaxKeyLen = 64 * 1024;
inline constexpr uint32_t kMaxValLen = 64 * 1024 * 1024;

enum class OpCode : uint8_t { PUT = 1, GET = 2, DEL = 3, PING = 4 };
enum class Status : uint8_t { OK = 0, NOT_FOUND = 1, ERROR = 2 };

#pragma pack(push, 1)
struct RequestHeader {
    uint32_t magic;    // kMagic
    uint8_t  opcode;   // OpCode
    uint32_t key_len;
    uint32_t val_len;
};

struct ResponseHeader {
    uint32_t magic;    // kMagic
    uint8_t  status;   // Status
    uint32_t val_len;
};
#pragma pack(pop)

static_assert(sizeof(RequestHeader) == 13, "RequestHeader must be packed");
static_assert(sizeof(ResponseHeader) == 9, "ResponseHeader must be packed");

#if defined(MSG_NOSIGNAL)
inline constexpr int kSendFlags = MSG_NOSIGNAL;
#else
inline constexpr int kSendFlags = 0;  // macOS: use SO_NOSIGPIPE via configure_socket()
#endif

// Writes exactly `len` bytes, retrying on partial sends and EINTR.
inline bool write_exact(int fd, const void* buf, size_t len) {
    const auto* p = static_cast<const char*>(buf);
    while (len > 0) {
        ssize_t n = ::send(fd, p, len, kSendFlags);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Reads exactly `len` bytes, retrying on partial reads and EINTR.
// Returns false on EOF, timeout, or error.
inline bool read_exact(int fd, void* buf, size_t len) {
    auto* p = static_cast<char*>(buf);
    while (len > 0) {
        ssize_t n = ::recv(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;  // peer closed
        p += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// Sets both receive and send timeouts so dead peers are detected quickly.
inline bool set_socket_timeout(int fd, int timeout_ms) {
    timeval tv{};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = static_cast<decltype(tv.tv_usec)>((timeout_ms % 1000) * 1000);
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0 &&
           ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == 0;
}

// Per-connection options: disable Nagle for small request/response frames
// and suppress SIGPIPE on platforms without MSG_NOSIGNAL.
inline void configure_socket(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#if defined(SO_NOSIGPIPE)
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
}

inline RequestHeader make_request(OpCode op, uint32_t key_len, uint32_t val_len) {
    return RequestHeader{kMagic, static_cast<uint8_t>(op), key_len, val_len};
}

inline ResponseHeader make_response(Status st, uint32_t val_len) {
    return ResponseHeader{kMagic, static_cast<uint8_t>(st), val_len};
}

}  // namespace kv
