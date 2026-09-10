#!/usr/bin/env python3
"""Summarize the sweep produced by benchmark/run_benchmarks.sh.

Prints two tables: throughput and latency percentiles vs. connection count,
and throughput vs. pipeline depth.

Usage: python3 benchmark/summarize_benchmarks.py [results_dir]
       results_dir defaults to benchmark/results
"""
import csv
import os
import sys

PERCENTILES = [50, 99, 99.9]


def load_latencies(path):
    if not os.path.exists(path):
        return []
    latencies = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            if len(row) >= 2:
                latencies.append(int(row[1]))
    latencies.sort()
    return latencies


def percentile(sorted_lat, p):
    if not sorted_lat:
        return None
    n = len(sorted_lat)
    if p >= 100:
        return sorted_lat[-1]
    idx = max(0, min(n - 1, int(-(-p * n // 100)) - 1))  # ceil(p/100 * n) - 1
    return sorted_lat[idx]


def load_summary(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def fmt(value):
    return f"{value:,}" if value is not None else "-"


def concurrent_table(results_dir):
    rows = load_summary(os.path.join(results_dir, "summary_concurrent.csv"))
    if not rows:
        print("no summary_concurrent.csv found — run benchmark/run_benchmarks.sh first\n")
        return

    print("Concurrent connections (per-op round-trip latency)")
    print(f"{'conns':>6}  {'total ops':>10}  {'throughput/s':>13}  "
          f"{'p50 ns':>10}  {'p99 ns':>10}  {'p99.9 ns':>11}")
    for row in rows:
        conns = row["connections"]
        lat = load_latencies(os.path.join(results_dir, f"concurrent_{conns}.csv"))
        p50, p99, p999 = (percentile(lat, p) for p in PERCENTILES)
        print(f"{conns:>6}  {int(row['total_ops']):>10,}  "
              f"{float(row['throughput_ops_sec']):>13,.0f}  "
              f"{fmt(p50):>10}  {fmt(p99):>10}  {fmt(p999):>11}")
    print()


def pipeline_table(results_dir):
    rows = load_summary(os.path.join(results_dir, "summary_pipeline.csv"))
    if not rows:
        print("no summary_pipeline.csv found — run benchmark/run_benchmarks.sh first\n")
        return

    baseline = None
    print("Pipeline depth (latency percentiles are per BATCH, not per op)")
    print(f"{'depth':>6}  {'total ops':>10}  {'throughput/s':>13}  {'vs depth 1':>10}  "
          f"{'p50 ns':>11}  {'p99 ns':>11}  {'p99.9 ns':>11}")
    for row in rows:
        depth = row["depth"]
        tput = float(row["throughput_ops_sec"])
        if baseline is None:
            baseline = tput
        speedup = f"{tput / baseline:.2f}x" if baseline else "-"
        lat = load_latencies(os.path.join(results_dir, f"pipeline_{depth}.csv"))
        p50, p99, p999 = (percentile(lat, p) for p in PERCENTILES)
        print(f"{depth:>6}  {int(row['total_ops']):>10,}  {tput:>13,.0f}  {speedup:>10}  "
              f"{fmt(p50):>11}  {fmt(p99):>11}  {fmt(p999):>11}")
    print()


def main():
    results_dir = sys.argv[1] if len(sys.argv) > 1 else "benchmark/results"
    if not os.path.isdir(results_dir):
        print(f"no such directory: {results_dir}")
        return 1
    print()
    concurrent_table(results_dir)
    pipeline_table(results_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
