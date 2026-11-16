#!/usr/bin/env bash
# End-to-end cluster test:
#   1. shard contention: 1 node with 1 shard vs 16 shards
#   2. 3-node cluster throughput benchmark
#   3. kill -9 a node and verify every replicated key is still readable
#   4. restart the killed node from its WAL and verify it recovered its keys
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"
DATA="$(mktemp -d "${TMPDIR:-/tmp}/kv-test.XXXXXX")"
THREADS="${THREADS:-16}"
OPS="${OPS:-100000}"
NODES="127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
    rm -rf "$DATA"
}
trap cleanup EXIT

jobs_count() { nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4; }

wait_for_port() {
    for _ in $(seq 1 50); do
        if (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; then return 0; fi
        sleep 0.1
    done
    echo "node on port $1 did not start" >&2
    return 1
}

# start_node <port> <wal> [num_shards] -> sets LAST_PID
start_node() {
    "$BUILD/node_server" "$1" "$2" "${3:-16}" >"$DATA/node-$1.out" 2>&1 &
    LAST_PID=$!
    PIDS+=("$LAST_PID")
    wait_for_port "$1"
}

stop_node() {
    kill "$1" 2>/dev/null || true
    wait "$1" 2>/dev/null || true
}

section() { printf '\n==== %s ====\n' "$1"; }

section "build"
cmake -S "$ROOT" -B "$BUILD" -DCMAKE_BUILD_TYPE=Release >/dev/null
cmake --build "$BUILD" -j "$(jobs_count)"

section "1. shard contention (single node, $THREADS threads, 50% PUT)"
for shards in 1 16; do
    start_node 8100 "$DATA/contention-$shards.log" "$shards"
    echo "-- num_shards=$shards"
    "$BUILD/benchmark" --mode=throughput --nodes=127.0.0.1:8100 --replicas=1 \
        --threads="$THREADS" --ops="$OPS" --put-ratio=0.5 | grep -E 'throughput:|latency:'
    stop_node "$LAST_PID"
done

section "2. 3-node cluster benchmark ($THREADS threads, $OPS ops, 20% PUT)"
start_node 8001 "$DATA/wal1.log"; NODE1=$LAST_PID
start_node 8002 "$DATA/wal2.log"
start_node 8003 "$DATA/wal3.log"
"$BUILD/benchmark" --mode=throughput --nodes="$NODES" --threads="$THREADS" --ops="$OPS"

section "3. node failover (kill -9 node 1)"
"$BUILD/benchmark" --mode=failover-write --nodes="$NODES" --keys=5000
kill -9 "$NODE1"
wait "$NODE1" 2>/dev/null || true
echo "killed node 1 (pid $NODE1)"
"$BUILD/benchmark" --mode=failover-read --nodes="$NODES" --keys=5000

section "4. WAL crash recovery (restart node 1)"
start_node 8001 "$DATA/wal1.log"
grep recovered "$DATA/node-8001.out"
"$BUILD/benchmark" --mode=recovery --nodes="$NODES" --target=127.0.0.1:8001 --keys=5000

section "all tests passed"
