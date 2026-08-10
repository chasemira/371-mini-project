"""Plot fairness throughput CSVs from results/ and save into pictures/."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt

HERE = Path(__file__).resolve().parent
RESULTS = HERE / "results"
PICTURES = HERE / "pictures"


def load(path: Path, max_gap_s: float = 3.0):
    """Return (active_elapsed_s, cumulative_bytes), skipping long sleep gaps."""
    times, bytes_ = [], []
    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            times.append(float(row["time"]))
            bytes_.append(int(row["cumulative_bytes"]))
    if not times:
        raise SystemExit(f"No data in {path}")
    elapsed = [0.0]
    for prev, cur in zip(times, times[1:]):
        dt = cur - prev
        step = dt if 0.0 < dt <= max_gap_s else 0.0
        elapsed.append(elapsed[-1] + step)
    return elapsed, bytes_


def jain(values):
    n = len(values)
    if n == 0 or sum(values) == 0:
        return 0.0
    return (sum(values) ** 2) / (n * sum(v * v for v in values))


def plot_trial(flow1: Path, flow2: Path, output: Path):
    rel1, b1 = load(flow1)
    rel2, b2 = load(flow2)
    x1 = b1[-1] / max(rel1[-1], 1e-9)
    x2 = b2[-1] / max(rel2[-1], 1e-9)
    J = jain([x1, x2])

    plt.figure(figsize=(9, 4.5))
    plt.plot(rel1, b1, label=f"Flow A ({x1:.1f} B/s)")
    plt.plot(rel2, b2, label=f"Flow B ({x2:.1f} B/s)")
    plt.xlabel("Active time (s)")
    plt.ylabel("Cumulative bytes received")
    plt.title(f"Fairness under shared bottleneck - Jain J={J:.3f}")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(output, dpi=160)
    plt.close()
    print(f"Flow A: {x1:.2f} B/s")
    print(f"Flow B: {x2:.2f} B/s")
    print(f"Jain's Fairness Index: {J:.3f}")
    print(f"Wrote {output}")
    return x1, x2, J


def plot_summary(summary_path: Path, output: Path):
    rows = list(csv.DictReader(summary_path.open(encoding="utf-8")))
    if not rows:
        raise SystemExit(f"No rows in {summary_path}")
    trials = [int(r["trial"]) for r in rows]
    a = [float(r["flowA_Bps"]) for r in rows]
    b = [float(r["flowB_Bps"]) for r in rows]
    js = [float(r["Jain"]) for r in rows]
    mean_J = sum(js) / len(js)
    min_J = min(js)
    max_J = max(js)

    x = range(len(trials))
    width = 0.35
    plt.figure(figsize=(9, 4.5))
    plt.bar([i - width / 2 for i in x], a, width, label="Flow A")
    plt.bar([i + width / 2 for i in x], b, width, label="Flow B")
    plt.xticks(list(x), [str(t) for t in trials])
    plt.xlabel("Trial")
    plt.ylabel("Throughput (B/s)")
    plt.title(
        f"Fairness across trials — Jain mean={mean_J:.3f}, "
        f"min={min_J:.3f}, max={max_J:.3f}"
    )
    plt.legend()
    plt.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(output, dpi=160)
    plt.close()
    print(f"Trials: {len(js)}")
    print(f"Mean Jain index: {mean_J:.3f}")
    print(f"Min Jain index:  {min_J:.3f}")
    print(f"Max Jain index:  {max_J:.3f}")
    print(f"Wrote {output}")


def pick_representative_trial(summary_path: Path):
    """Prefer a trial with typical duration (avoids sleep/hung outliers)."""
    rows = list(csv.DictReader(summary_path.open(encoding="utf-8")))
    if not rows:
        return 1
    durs = sorted(
        (max(float(r["flowA_s"]), float(r["flowB_s"])), int(r["trial"]))
        for r in rows
    )
    return durs[len(durs) // 2][1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow1", default=None)
    parser.add_argument("--flow2", default=None)
    parser.add_argument(
        "--output",
        default=str(PICTURES / "fairness_throughput.png"),
    )
    parser.add_argument(
        "--summary",
        default=str(RESULTS / "fairness_summary.csv"),
        help="If present, also plot per-trial throughput bars",
    )
    parser.add_argument("--trial", type=int, default=None,
                        help="Which trial's cumulative curves to plot")
    args = parser.parse_args()

    PICTURES.mkdir(parents=True, exist_ok=True)
    summary_path = Path(args.summary)

    trial = args.trial
    if trial is None and summary_path.exists():
        trial = pick_representative_trial(summary_path)
    if trial is None:
        trial = 1

    flow1 = Path(args.flow1) if args.flow1 else RESULTS / f"throughput_flow1_t{trial}.csv"
    flow2 = Path(args.flow2) if args.flow2 else RESULTS / f"throughput_flow2_t{trial}.csv"
    print(f"Plotting cumulative curves for trial {trial}")
    plot_trial(flow1, flow2, Path(args.output))

    if summary_path.exists():
        plot_summary(summary_path, PICTURES / "fairness_trials.png")


if __name__ == "__main__":
    main()
