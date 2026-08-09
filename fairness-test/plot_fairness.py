"""Plot fairness throughput CSVs from results/ and save into pictures/."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
PICTURES = HERE / "pictures"


def load(path: Path):
    times, bytes_ = [], []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            times.append(float(row["time"]))
            bytes_.append(int(row["cumulative_bytes"]))
    if not times:
        raise SystemExit(f"No data in {path}")
    return times, bytes_


def jain(values):
    n = len(values)
    if n == 0 or sum(values) == 0:
        return 0.0
    return (sum(values) ** 2) / (n * sum(v * v for v in values))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--flow1",
        default=str(RESULTS / "throughput_flow1_t1.csv"),
    )
    parser.add_argument(
        "--flow2",
        default=str(RESULTS / "throughput_flow2_t1.csv"),
    )
    parser.add_argument(
        "--output",
        default=str(PICTURES / "fairness_throughput.png"),
    )
    args = parser.parse_args()

    PICTURES.mkdir(parents=True, exist_ok=True)

    t1, b1 = load(Path(args.flow1))
    t2, b2 = load(Path(args.flow2))
    t0 = min(t1[0], t2[0])
    rel1 = [t - t0 for t in t1]
    rel2 = [t - t0 for t in t2]
    x1 = b1[-1] / max(rel1[-1], 1e-9)
    x2 = b2[-1] / max(rel2[-1], 1e-9)
    J = jain([x1, x2])

    plt.figure(figsize=(9, 4.5))
    plt.plot(rel1, b1, label=f"Flow A ({x1:.1f} B/s)")
    plt.plot(rel2, b2, label=f"Flow B ({x2:.1f} B/s)")
    plt.xlabel("Time (s)")
    plt.ylabel("Cumulative bytes received")
    plt.title(f"Fairness under shared bottleneck - Jain J={J:.3f}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(args.output, dpi=160)
    print(f"Flow A: {x1:.2f} B/s")
    print(f"Flow B: {x2:.2f} B/s")
    print(f"Jain's Fairness Index: {J:.3f}")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
