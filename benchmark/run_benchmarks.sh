#!/usr/bin/env bash
# ============================================================================
# Builds the server and all benchmark tools, then runs the full sweep:
#   - concurrent connections: 1 / 10 / 50 / 100 / 200
#   - pipeline depth:         1 / 2 / 4 / 8 / 16 / 32 / 64
#   - an adversarial split-write parser correctness run
#
# The server is restarted for every single run, so each measurement starts
# against an empty hash table — otherwise keys accumulate across the sweep and
# later runs get charged for a bigger table than earlier ones.
#
# Results land in benchmark/results/ (gitignored). Summarize them with:
#   python3 benchmark/summarize_benchmarks.py
#
# Env overrides:
#   PORT=6379  OPS_TOTAL=200000  ADVERSARIAL_OPS=400
# ============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

PORT="${PORT:-6379}"
OPS_TOTAL="${OPS_TOTAL:-200000}"
ADVERSARIAL_OPS="${ADVERSARIAL_OPS:-400}"

RESULTS="benchmark/results"
SERVER_BIN="/tmp/redisclone_bench_server"
SERVER_LOG="/tmp/redisclone_bench_server.log"
SERVER_PID=""

CONNECTION_COUNTS=(1 10 50 100 200)
PIPELINE_DEPTHS=(1 2 4 8 16 32 64)

# 200 concurrent connections needs headroom on both sides of the loopback.
ulimit -n 4096 2>/dev/null || true

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}
trap stop_server EXIT INT TERM

wait_for_port() {
    for _ in $(seq 1 100); do
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done
    echo "server never came up on port $PORT" >&2
    cat "$SERVER_LOG" >&2 || true
    exit 1
}

start_server() {
    stop_server
    "$SERVER_BIN" > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    wait_for_port
}

# Pulls "key: value" lines out of a tool's stdout.
field() {
    awk -v k="$1" -F': ' '$1 == k { print $2; exit }'
}

echo "=== building ==="
mkdir -p "$RESULTS"
clang++ -std=c++17 -O2 src/resp_server.cpp -o "$SERVER_BIN"
clang++ -std=c++17 -O2 benchmark/bench_client.cpp -o benchmark/bench_client
clang++ -std=c++17 -O2 -pthread benchmark/bench_concurrent.cpp -o benchmark/bench_concurrent
clang++ -std=c++17 -O2 benchmark/bench_pipeline.cpp -o benchmark/bench_pipeline
echo "built server + 3 benchmark tools"
echo

echo "=== concurrent connection sweep (total ops per run: $OPS_TOTAL) ==="
echo "connections,ops_per_connection,total_ops,wall_seconds,throughput_ops_sec" > "$RESULTS/summary_concurrent.csv"
for n in "${CONNECTION_COUNTS[@]}"; do
    per_conn=$(( OPS_TOTAL / n ))
    [[ "$per_conn" -lt 1 ]] && per_conn=1
    start_server
    out="$RESULTS/concurrent_${n}.csv"
    printf 'connections=%-4s ops/conn=%-7s ... ' "$n" "$per_conn"
    result=$(./benchmark/bench_concurrent "$PORT" "$n" "$per_conn" "$out")
    total=$(echo "$result" | field "total_ops")
    wall=$(echo "$result" | field "wall_seconds")
    tput=$(echo "$result" | field "throughput_ops_sec")
    echo "$n,$per_conn,$total,$wall,$tput" >> "$RESULTS/summary_concurrent.csv"
    printf '%s ops/sec\n' "$tput"
done
echo

echo "=== pipeline depth sweep (total ops per run: $OPS_TOTAL) ==="
echo "depth,total_ops,wall_seconds,throughput_ops_sec" > "$RESULTS/summary_pipeline.csv"
for d in "${PIPELINE_DEPTHS[@]}"; do
    start_server
    out="$RESULTS/pipeline_${d}.csv"
    printf 'depth=%-4s ... ' "$d"
    result=$(./benchmark/bench_pipeline "$PORT" "$d" "$OPS_TOTAL" "$out")
    total=$(echo "$result" | field "total_ops")
    wall=$(echo "$result" | field "wall_seconds")
    tput=$(echo "$result" | field "throughput_ops_sec")
    verdict=$(echo "$result" | field "validation")
    echo "$d,$total,$wall,$tput" >> "$RESULTS/summary_pipeline.csv"
    printf '%s ops/sec  [%s]\n' "$tput" "${verdict%% *}"
done
echo

echo "=== adversarial split-write parser check ==="
# Correctness only — the deliberate sleeps between split writes make the
# throughput number meaningless, so it is not recorded.
for d in 4 16 64; do
    start_server
    printf 'depth=%-4s split writes ... ' "$d"
    result=$(./benchmark/bench_pipeline "$PORT" "$d" "$ADVERSARIAL_OPS" \
             "$RESULTS/adversarial_${d}.csv" --adversarial)
    writes=$(echo "$result" | field "write_calls")
    bytes=$(echo "$result" | field "bytes_written")
    verdict=$(echo "$result" | field "validation")
    printf '%s bytes over %s write() calls  [%s]\n' "$bytes" "$writes" "${verdict%% *}"
done
echo

stop_server
echo "=== done ==="
echo "results in $RESULTS/"
echo "summarize with: python3 benchmark/summarize_benchmarks.py"
