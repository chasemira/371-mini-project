"""
tcp_comparison.py: Run our protocol alongside real TCP traffic.

Method:
  1. Measure our protocol alone.
  2. Start a saturating TCP transfer on loopback (a plain socket server and
     client moving data as fast as they can), and measure our protocol again
     while that load is running.
  3. Compare transfer time, throughput and correctness between the two.

We also record the TCP flow's own throughput over the same window, so the two
transports can be compared rather than only observing that ours slowed down.

"""

import filecmp
import os
import re
import socket
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = os.path.join(HERE, "venv", "bin", "python")
TCP_PORT = 9500
CHUNK = 64 * 1024


class TCPLoad:
    """A saturating TCP transfer on loopback, used as competing traffic."""

    def __init__(self):
        self.stop = threading.Event()
        self.bytes_sent = 0
        self.started = None
        self._threads = []

    def _server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", TCP_PORT))
        srv.listen(4)
        srv.settimeout(0.5)
        while not self.stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            # drain whatever the client sends until it goes away
            with conn:
                while not self.stop.is_set():
                    try:
                        if not conn.recv(CHUNK):
                            break
                    except OSError:
                        break
        srv.close()

    def _client(self):
        payload = b"x" * CHUNK
        while not self.stop.is_set():
            try:
                s = socket.create_connection(("127.0.0.1", TCP_PORT), timeout=2)
            except OSError:
                time.sleep(0.05)
                continue
            with s:
                while not self.stop.is_set():
                    try:
                        s.sendall(payload)
                        self.bytes_sent += CHUNK
                    except OSError:
                        break

    def start(self):
        for target in (self._server, self._client, self._client):
            t = threading.Thread(target=target, daemon=True)
            t.start()
            self._threads.append(t)
        time.sleep(1.0)          # let the connections come up
        self.bytes_sent = 0      # only count bytes moved during measurement
        self.started = time.time()

    def finish(self):
        elapsed = time.time() - self.started
        self.stop.set()
        for t in self._threads:
            t.join(timeout=3)
        return self.bytes_sent / elapsed if elapsed else 0.0


def run_protocol():
    """One full transfer. Returns (seconds, ok)."""
    env = dict(os.environ, MPLBACKEND="Agg", PYTHONUNBUFFERED="1")
    out = os.path.join(HERE, "received_packets.txt")
    if os.path.exists(out):
        os.remove(out)

    rx = subprocess.Popen([PY, "receiver.py"], cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.6)

    t0 = time.time()
    subprocess.run([PY, "sender.py"], cwd=HERE, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300)
    elapsed = time.time() - t0

    try:
        rx.wait(timeout=10)
    except subprocess.TimeoutExpired:
        rx.kill()

    time.sleep(0.3)  # let the receiver flush its final write
    ok = os.path.exists(out) and filecmp.cmp(
        os.path.join(HERE, "data.txt"), out, shallow=False)
    return elapsed, ok


def set_loss(rate):
    """Rewrite LOSS_PROBABILITY in sender.py."""
    path = os.path.join(HERE, "sender.py")
    src = open(path).read()
    src = re.sub(r"^LOSS_PROBABILITY = [\d.]+", f"LOSS_PROBABILITY = {rate}", src, count=1, flags=re.M)
    open(path, "w").write(src)


def measure(trials):
    """Run the protocol `trials` times. Returns (times, correct_count)."""
    times, oks = [], 0
    for _ in range(trials):
        el, ok = run_protocol()
        times.append(el)
        oks += int(ok)
        time.sleep(0.3)
    return times, oks


def report(label, times, oks, size):
    avg = sum(times) / len(times)
    print(f"    {label:<16} avg {avg:6.2f}s  (min {min(times):5.2f}  "f"max {max(times):5.2f})   {size/avg:8.1f} B/s   correct {oks}/{len(times)}")
    return avg


def main():
    trials = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    size = os.path.getsize(os.path.join(HERE, "data.txt"))
    original = open(os.path.join(HERE, "sender.py")).read()
    rows = []

    print(f"{size}-byte transfer, {trials} trials per condition\n")
    try:
        # test under 0% and 35% L/C rate
        for rate in (0.0, 0.35):
            set_loss(rate)
            print(f"loss rate {rate:.0%}")

            alone_t, alone_ok = measure(trials)
            alone_avg = report("alone", alone_t, alone_ok, size)

            load = TCPLoad()
            load.start()
            with_t, with_ok = measure(trials)
            tcp_bps = load.finish()
            with_avg = report("with TCP load", with_t, with_ok, size)

            print(f"    competing TCP: {tcp_bps/1e6:.0f} MB/s      "
                  f"slowdown: {with_avg/alone_avg:.2f}x\n")
            rows.append((rate, alone_avg, with_avg, alone_ok, with_ok, tcp_bps))
    finally:
        open(os.path.join(HERE, "sender.py"), "w").write(original)

    with open(os.path.join(HERE, "tcp_comparison.csv"), "w") as f:
        f.write("loss_rate,alone_avg_s,with_tcp_avg_s,slowdown,"
                "correct_alone,correct_with_tcp,trials,tcp_Bps\n")
        for r, a, w, ao, wo, tb in rows:
            f.write(f"{r},{a:.2f},{w:.2f},{w/a:.2f},{ao},{wo},{trials},{tb:.0f}\n")
    print("wrote tcp_comparison.csv; sender.py restored")


if __name__ == "__main__":
    main()
