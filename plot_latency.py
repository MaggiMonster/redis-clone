#!/usr/bin/env python3
"""Compare per-operation SET latency between the incremental and naive
(stop-the-world) hash table resize strategies.

Usage: python3 plot_latency.py [incremental.csv] [naive.csv]
"""
import csv
import sys
import matplotlib.pyplot as plt

INCREMENTAL_COLOR = "#2a78d6"  # categorical slot 1 (blue)
NAIVE_COLOR = "#008300"        # categorical slot 2 (green)
GRID_COLOR = "#e1e0d9"
AXIS_COLOR = "#c3c2b7"
TEXT_COLOR = "#0b0b0b"
SECONDARY_TEXT = "#52514e"


def load_csv(path):
    indices = []
    latencies = []
    with open(path, newline="") as f:
        reader = csv.reader(f)
        next(reader)  # header
        for row in reader:
            indices.append(int(row[0]))
            latencies.append(int(row[1]))
    return indices, latencies


def main():
    incremental_path = sys.argv[1] if len(sys.argv) > 1 else "incremental.csv"
    naive_path = sys.argv[2] if len(sys.argv) > 2 else "naive.csv"

    inc_idx, inc_lat = load_csv(incremental_path)
    naive_idx, naive_lat = load_csv(naive_path)

    inc_max = max(inc_lat)
    naive_max = max(naive_lat)

    print(f"incremental max latency_ns: {inc_max:,}")
    print(f"naive       max latency_ns: {naive_max:,}")

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#fcfcfb")
    ax.set_facecolor("#fcfcfb")

    # Scatter, not connected lines: these are independent per-op samples,
    # not a continuous quantity, and a line would imply interpolation
    # between unrelated points.
    ax.scatter(inc_idx, inc_lat, s=1, alpha=0.3, color=INCREMENTAL_COLOR,
               label="incremental", rasterized=True, linewidths=0)
    ax.scatter(naive_idx, naive_lat, s=1, alpha=0.3, color=NAIVE_COLOR,
               label="naive (stop-the-world)", rasterized=True, linewidths=0)

    ax.set_xlabel("operation index", color=SECONDARY_TEXT)
    ax.set_ylabel("latency (ns)", color=SECONDARY_TEXT)
    ax.set_title("SET latency per operation: incremental vs. stop-the-world rehashing",
                 color=TEXT_COLOR)

    ax.grid(True, color=GRID_COLOR, linewidth=0.5)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(AXIS_COLOR)
    ax.spines["bottom"].set_color(AXIS_COLOR)
    ax.tick_params(colors=SECONDARY_TEXT)

    legend = ax.legend(loc="upper right", frameon=False, markerscale=8)
    for text in legend.get_texts():
        text.set_color(TEXT_COLOR)

    fig.tight_layout()
    out_path = "latency_comparison.png"
    fig.savefig(out_path, dpi=150)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
