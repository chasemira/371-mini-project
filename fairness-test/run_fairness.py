"""
Bonus fairness experiment launcher.

Starts:
  1) shared bottleneck relay
  2) two receivers (ports 9200 / 9202)
  3) two senders (ports 9001 / 9003) pointed at the relay

Artifacts go under:
  fairness-test/results/   (csv + txt)
  fairness-test/pictures/  (png)

Run from project root:
  python fairness-test/run_fairness.py
  python fairness-test/run_fairness.py --trials 5
"""

from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
RESULTS = HERE / "results"
PICTURES = HERE / "pictures"
PY = sys.executable


def jain(values):
    values = [v for v in values if v > 0]
    n = len(values)
    if n == 0:
        return 0.0
    s = sum(values)
    return (s * s) / (n * sum(v * v for v in values))


def avg_throughput(path: Path):
    rows = list(csv.DictReader(path.open(encoding="utf-8")))
    if len(rows) < 2:
        if not rows:
            return 0.0, 0, 0.0
        return 0.0, int(rows[-1]["cumulative_bytes"]), 0.0
    t0 = float(rows[0]["time"])
    t1 = float(rows[-1]["time"])
    b = int(rows[-1]["cumulative_bytes"])
    dur = max(t1 - t0, 1e-9)
    return b / dur, b, dur


def spawn(args, cwd: Path):
    print(">", " ".join(args))
    return subprocess.Popen(args, cwd=str(cwd))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--queue-size", type=int, default=4)
    parser.add_argument("--service-ms", type=float, default=35.0)
    args = parser.parse_args()

    RESULTS.mkdir(parents=True, exist_ok=True)
    PICTURES.mkdir(parents=True, exist_ok=True)

    payload_a = PROJECT / "data.txt"
    payload_b = HERE / "data_b.txt"

    summary = []
    for trial in range(1, args.trials + 1):
        print(f"\n===== FAIRNESS TRIAL {trial}/{args.trials} =====")
        tp1 = RESULTS / f"throughput_flow1_t{trial}.csv"
        tp2 = RESULTS / f"throughput_flow2_t{trial}.csv"
        out1 = RESULTS / f"received_a_t{trial}.txt"
        out2 = RESULTS / f"received_b_t{trial}.txt"
        log_a = RESULTS / f"cwnd_log_a_t{trial}.csv"
        log_b = RESULTS / f"cwnd_log_b_t{trial}.csv"
        plot_a = PICTURES / f"cwnd_a_t{trial}.png"
        plot_b = PICTURES / f"cwnd_b_t{trial}.png"

        relay = spawn(
            [
                PY, str(HERE / "bottleneck_relay.py"),
                "--queue-size", str(args.queue_size),
                "--service-ms", str(args.service_ms),
            ],
            cwd=HERE,
        )
        time.sleep(0.4)

        # sender/receiver live in project root (need header.py imports + data.txt)
        recv_a = spawn(
            [
                PY, "receiver.py", "--port", "9200",
                "--output", str(out1), "--throughput", str(tp1),
            ],
            cwd=PROJECT,
        )
        recv_b = spawn(
            [
                PY, "receiver.py", "--port", "9202",
                "--output", str(out2), "--throughput", str(tp2),
            ],
            cwd=PROJECT,
        )
        time.sleep(0.4)

        send_a = spawn(
            [
                PY, "sender.py",
                "--sender-port", "9001", "--receiver-port", "9100",
                "--payload", str(payload_a), "--loss", "0.0",
                "--cwnd-log", str(log_a),
                "--cwnd-plot", str(plot_a),
                "--no-plot",
            ],
            cwd=PROJECT,
        )
        send_b = spawn(
            [
                PY, "sender.py",
                "--sender-port", "9003", "--receiver-port", "9102",
                "--payload", str(payload_b), "--loss", "0.0",
                "--cwnd-log", str(log_b),
                "--cwnd-plot", str(plot_b),
                "--no-plot",
            ],
            cwd=PROJECT,
        )

        codes = [send_a.wait(), send_b.wait()]
        time.sleep(0.5)
        for proc in (recv_a, recv_b):
            try:
                proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.terminate()
        relay.terminate()
        try:
            relay.wait(timeout=3)
        except subprocess.TimeoutExpired:
            relay.kill()

        if any(c != 0 for c in codes):
            print("WARNING: a sender exited non-zero:", codes)

        x1, b1, d1 = avg_throughput(tp1)
        x2, b2, d2 = avg_throughput(tp2)
        J = jain([x1, x2])
        summary.append((trial, x1, x2, b1, b2, d1, d2, J))
        print(
            f"Trial {trial}: A={x1:.1f} B/s ({b1} B in {d1:.2f}s), "
            f"B={x2:.1f} B/s ({b2} B in {d2:.2f}s), J={J:.3f}"
        )

    print("\n===== SUMMARY =====")
    print("trial,flowA_Bps,flowB_Bps,Jain")
    for trial, x1, x2, b1, b2, d1, d2, J in summary:
        print(f"{trial},{x1:.2f},{x2:.2f},{J:.3f}")
    if summary:
        mean_J = sum(r[-1] for r in summary) / len(summary)
        print(f"Mean Jain index: {mean_J:.3f}")
        print("Next: python fairness-test/plot_fairness.py")


if __name__ == "__main__":
    main()
