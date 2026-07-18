#!/usr/bin/env python3
"""Compare per-operation SET latency between the incremental and naive
(stop-the-world) hash table resize strategies as a percentile table.

Usage: python3 latency_percentiles.py [incremental.csv] [naive.csv]
"""
import csv
import sys

PERCENTILES = [50, 75, 90, 95, 99, 99.5, 99.9, 99.95, 99.99, 100]


def load_csv(path):
    latencies = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for row in reader:
            latencies.append(int(row[1]))
    return latencies


def percentile(sorted_lat, p):
    n = len(sorted_lat)
    if p >= 100:
        return sorted_lat[-1]
    idx = max(0, min(n - 1, int(-(-p * n // 100)) - 1))  # ceil(p/100 * n) - 1
    return sorted_lat[idx]


def main():
    incremental_path = sys.argv[1] if len(sys.argv) > 1 else "incremental.csv"
    naive_path = sys.argv[2] if len(sys.argv) > 2 else "naive.csv"

    inc_lat = sorted(load_csv(incremental_path))
    naive_lat = sorted(load_csv(naive_path))

    print(f"{'percentile':>11}  {'incremental_ns':>15}  {'naive_ns':>15}")
    for p in PERCENTILES:
        label = f"p{p:g}"
        inc_v = percentile(inc_lat, p)
        naive_v = percentile(naive_lat, p)
        print(f"{label:>11}  {inc_v:>15,}  {naive_v:>15,}")


if __name__ == "__main__":
    main()
