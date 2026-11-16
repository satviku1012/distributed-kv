# simple-distributed-kv

A lightweight, crash-resilient, sharded distributed key-value store written in C++20
with zero third-party dependencies (C++ standard library + POSIX sockets/file I/O only).

## Architecture

- **Consistent hashing** (`include/hash_ring.h`): FNV-1a hash ring with 32 virtual nodes
  per physical server; each key is replicated to the first R distinct nodes clockwise (default R = 2).
- **Sharded storage** (`include/sharded_store.h`): each node splits its keyspace across 16
  shards guarded by `std::shared_mutex`, so reads never block each other and writes lock 1/16 of the keyspace.
- **Binary WAL**: every `PUT`/`DEL` is appended as a checksummed, length-prefixed frame before the
  in-memory shard is mutated. On startup the WAL is replayed and any torn tail record is truncated.
- **Binary wire protocol** (`include/protocol.h`): packed request/response headers over persistent
  TCP connections, with `read_exact`/`write_exact` helpers that handle partial reads and writes.

## Layout

```text
include/   protocol.h, hash_ring.h, sharded_store.h, cluster_client.h
src/       node_server.cpp, benchmark.cpp
scripts/   run_cluster_test.sh
```

## Building

```sh
mkdir -p build && cd build
cmake .. && make -j
```

## Status

- [x] wire protocol and socket helpers
- [x] consistent hash ring
- [x] sharded store with binary WAL and crash recovery
- [ ] node server
- [ ] cluster client with replica failover
- [ ] benchmark and end-to-end cluster test
