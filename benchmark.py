"""
benchmark.py: Measure transfer time and throughput across loss rates.

Runs the protocol end to end several times at each loss rate, verifies the
received file is byte-identical to the source, and prints a table for the
report. Raw per-trial data is written to results/benchmark.csv and the console
output to results/benchmark_output.txt, so the reported numbers can be checked
and regenerated rather than taken on trust.

Uses sender.py's --loss flag, so no source file is modified while running.

Usage:
    ./venv/bin/python benchmark.py [trials]
"""

import filecmp
import os
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = os.path.join(HERE, "venv", "bin", "python")
PAYLOAD = os.path.join(HERE, "data.txt")
OUTPUT = os.path.join(HERE, "results", "received_packets.txt")
CSV_PATH = os.path.join(HERE, "results", "benchmark.csv")
LOG_PATH = os.path.join(HERE, "results", "benchmark_output.txt")

LOSS_RATES = [0.0, 0.02, 0.10, 0.20, 0.35]


class Tee:
    """Write to stdout and to the evidence log at the same time."""

    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.f = open(path, "w", encoding="utf-8")

    def write(self, text):
        sys.__stdout__.write(text)
        self.f.write(text)

    def flush(self):
        sys.__stdout__.flush()
        self.f.flush()


def one_run(loss):
    """One full transfer at the given loss rate. Returns (seconds, ok, retransmits)."""
    env = dict(os.environ, MPLBACKEND="Agg", PYTHONUNBUFFERED="1")
    if os.path.exists(OUTPUT):
        os.remove(OUTPUT)

    rx = subprocess.Popen([PY, "receiver.py"], cwd=HERE, env=env,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.6)

    t0 = time.time()
    tx = subprocess.run([PY, "sender.py", "--loss", str(loss), "--no-plot"],
                        cwd=HERE, env=env, capture_output=True, text=True, timeout=300)
    elapsed = time.time() - t0

    try:
        rx.wait(timeout=10)
    except subprocess.TimeoutExpired:
        rx.kill()

    time.sleep(0.3)  # let the receiver flush its final write
    ok = os.path.exists(OUTPUT) and filecmp.cmp(PAYLOAD, OUTPUT, shallow=False)
    return elapsed, ok, tx.stdout.count("[REXMIT]")


def main():
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    sys.stdout = Tee(LOG_PATH)

    size = os.path.getsize(PAYLOAD)
    print(f"benchmark: {size}-byte payload, {trials} trials per loss rate")
    print(f"run at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    rows = []
    for loss in LOSS_RATES:
        times, rex, oks = [], [], 0
        for i in range(trials):
            el, ok, r = one_run(loss)
            times.append(el)
            rex.append(r)
            oks += int(ok)
            print(f"  loss={loss:<5} trial {i+1}: {el:6.2f}s  "
                  f"retransmits={r:<3} correct={ok}")
            time.sleep(0.3)

        avg = sum(times) / len(times)
        rows.append((loss, avg, min(times), max(times), size / avg,
                     sum(rex) / len(rex), oks, trials))
        print(f"  -> loss={loss:<5} avg={avg:6.2f}s  min={min(times):5.2f}  "
              f"max={max(times):5.2f}  {size/avg:7.1f} B/s  "
              f"rexmit={sum(rex)/len(rex):5.1f}  correct={oks}/{trials}\n")

    print("=" * 78)
    print(f"{'Loss':<8}{'Avg':>8}{'Min':>8}{'Max':>8}{'Throughput':>14}"
          f"{'Retransmits':>14}{'Correct':>10}")
    print("=" * 78)
    total_ok = 0
    for loss, avg, lo, hi, bps, r, oks, n in rows:
        total_ok += oks
        print(f"{loss:<8.0%}{avg:>7.2f}s{lo:>7.2f}s{hi:>7.2f}s"
              f"{bps:>11.1f} B/s{r:>14.1f}{oks:>7}/{n}")
    print("=" * 78)
    print(f"\n{total_ok}/{sum(r[7] for r in rows)} transfers byte-identical to source")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", encoding="utf-8") as f:
        f.write("loss_rate,avg_s,min_s,max_s,throughput_Bps,"
                "avg_retransmits,correct,trials\n")
        for r in rows:
            f.write(",".join(f"{x:.4g}" if isinstance(x, float) else str(x)
                             for x in r) + "\n")
    print(f"\nwrote {os.path.relpath(CSV_PATH, HERE)} "
          f"and {os.path.relpath(LOG_PATH, HERE)}")


if __name__ == "__main__":
    main()
