# distributed-kv

[![CI](https://github.com/satviku1012/distributed-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/satviku1012/distributed-kv/actions/workflows/ci.yml)

A sharded, replicated key-value store written in C++20 with **no third-party libraries**:
just the standard library and POSIX sockets and file I/O.

- **Partitioning and replication:** keys are spread across nodes with consistent hashing and
  written to 2 nodes, so the cluster keeps serving reads when a node dies.
- **Concurrency:** each node splits its data into 16 independently locked shards, so reads
  never block each other.
- **Durability:** every write goes to a checksummed write-ahead log before it's applied, and a
  crashed node rebuilds its state from that log on restart.

## Results

Measured with `scripts/run_cluster_test.sh` on Linux (Docker, 11 cores), with all nodes on
one machine over localhost. 16 client threads, 100,000 operations.

| Test | Result |
|---|---|
| 3-node cluster, 20% writes | 106K–158K ops/sec, p99 333–734 µs |
| `kill -9` one node, read back 5,000 keys | **5,000 / 5,000** (100%) |
| Restart the killed node from its WAL | ~24.7K records replayed in **~4.5 ms**, all 3,514 of its keys back |
| 1 shard vs 16 shards (one node, 50% writes) | **~2× throughput** (88K → 181K–196K ops/sec), **~4× lower p99** (1.07 ms → 262–304 µs) |

The ranges cover two runs. Because the nodes share one machine, these numbers measure the
software, not a real network.

## Quick start

Requires a C++20 compiler and CMake 3.16+. Runs on Linux and macOS.

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j

# start a 3-node cluster
./build/node_server 8001 wal1.log &
./build/node_server 8002 wal2.log &
./build/node_server 8003 wal3.log &

# benchmark it
./build/benchmark --mode=throughput \
    --nodes=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --threads=16 --ops=100000
```

Stop the nodes with `kill %1 %2 %3`. They flush their logs on `SIGINT` / `SIGTERM`.

## Testing

```sh
ctest --test-dir build --output-on-failure   # unit tests
./scripts/run_cluster_test.sh                # end-to-end suite
```

The unit tests cover the hash ring (distinct replicas, minimal key movement when a node
leaves, even distribution) and the WAL (replay, torn-write truncation, corrupt records,
concurrent writers). The end-to-end script runs the four tests in the results table. CI runs
both on Ubuntu and macOS for every push.

## How it works

```text
          client (one ClusterClient per thread)
             │  hash ring picks 2 nodes per key
     ┌───────┼────────────────┐
     ▼       ▼                ▼
  node 8001  node 8002     node 8003
     │
     ├─ 16 shards, each a hash map + reader-writer lock
     └─ write-ahead log on disk
```

**Consistent hashing** ([`include/hash_ring.h`](include/hash_ring.h)). Each node is placed at
32 points ("virtual nodes") on a 32-bit ring. A key belongs to the first node clockwise from
its hash, and its replica to the next *different* node. Adding or removing a node only moves
the keys next to its points. Positions use FNV-1a followed by the MurmurHash3 finalizer:
FNV-1a alone left one node owning 61% of keys, because names like `host#v0` and `host#v1`
landed close together.

**Client** ([`include/cluster_client.h`](include/cluster_client.h)). Writes go to both
replicas and succeed once either one acknowledges. Reads go to the primary and fall back to
the replica if the primary is unreachable. Every connect, read and write has a 200 ms
timeout, so a dead node costs at most one timeout before failing over.

**Storage engine** ([`include/sharded_store.h`](include/sharded_store.h)). A key's shard is
`hash(key) % 16`. Each shard has its own `std::shared_mutex` and is aligned to a 64-byte
cache line so shards don't contend on the same line. A write holds its shard's lock while
appending to the log, so the log order always matches the in-memory order for each key.

**Write-ahead log.** Each record is:

```text
[checksum 4B][op 1B][key length 4B][value length 4B][key][value]
```

On startup, the node replays records until it finds a truncated or corrupt one (from a crash
mid-write), then cuts the file back to the last good record. Writes reach the OS on every
call, which survives a process crash. The store's `fsync_each_write` option also survives
power loss, at a large cost in throughput. The node server doesn't expose it yet.

**Wire protocol** ([`include/protocol.h`](include/protocol.h)). Requests and responses are
packed binary headers followed by the key and value, over persistent TCP connections.
`read_exact` / `write_exact` loop until every byte has moved, because TCP can split a message
anywhere.

## Limitations

This is a learning project, and some important pieces of a production store are missing:

- **No repair after downtime.** A restarted node doesn't fetch the writes it missed while it
  was down, so it can return stale or missing values for those keys. Production systems fix
  this with read repair or anti-entropy (e.g. Merkle trees).
- **Weak consistency.** A write succeeds once one replica acknowledges, and there are no
  quorums or version numbers to settle conflicting writes.
- **Fixed membership.** The node list is passed on the command line. There's no failure
  detection, and no rebalancing when nodes join or leave.
- **The log grows forever.** There's no compaction or snapshotting.

## Project layout

```text
include/
  protocol.h        wire format and socket helpers
  hash_ring.h       consistent hash ring
  sharded_store.h   sharded store and write-ahead log
  cluster_client.h  routing, replication and failover
src/
  node_server.cpp   storage node
  benchmark.cpp     load generator and failover checks
tests/
  unit_tests.cpp    hash ring and WAL tests
scripts/
  run_cluster_test.sh
```
