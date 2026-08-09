"""
Shared finite-queue UDP relay for the fairness bonus.

Two PRTP senders send through this relay instead of directly to their receivers.
DATA packets share one FIFO queue (capacity limited). When the queue is full,
arriving DATA is dropped (= congestion loss). Control / ACK / FIN packets are
forwarded immediately so connection setup still works.

Topology (default):
  Flow A: sender:9001  <->  relay:9100  <->  receiver:9200
  Flow B: sender:9003  <->  relay:9102  <->  receiver:9202

Run from anywhere:
  python fairness-test/bottleneck_relay.py
"""

from __future__ import annotations

import argparse
import queue
import select
import socket
import sys
import threading
import time
from pathlib import Path

# Allow importing header.py from the project root
PROJECT = Path(__file__).resolve().parent.parent
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from header import PacketHeader

HOST = "127.0.0.1"


def is_data_plane(raw: bytes) -> bool:
    """True for pure application DATA (no SYN/ACK/FIN). Those enter the queue."""
    try:
        pkt = PacketHeader.from_bytes(raw)
    except Exception:
        return True
    return not (pkt.syn or pkt.ack or pkt.fin)


class FlowPath:
    def __init__(self, listen_port: int, receiver_port: int, name: str):
        self.name = name
        self.listen_port = listen_port
        self.receiver = (HOST, receiver_port)
        self.sender_addr = None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((HOST, listen_port))
        self.sock.setblocking(False)
        self.drops = 0
        self.forwarded = 0


def drain_queue(shared_q: queue.Queue, service_s: float, stop: threading.Event):
    """Serve queued DATA at a fixed rate (creates the bottleneck)."""
    while not stop.is_set():
        try:
            flow, raw = shared_q.get(timeout=0.05)
        except queue.Empty:
            continue
        try:
            flow.sock.sendto(raw, flow.receiver)
            flow.forwarded += 1
        except OSError:
            pass
        time.sleep(service_s)


def main():
    parser = argparse.ArgumentParser(description="PRTP shared-bottleneck relay")
    parser.add_argument("--queue-size", type=int, default=4)
    parser.add_argument("--service-ms", type=float, default=35.0,
                        help="Milliseconds between forwarding queued DATA packets")
    parser.add_argument("--a-listen", type=int, default=9100)
    parser.add_argument("--a-receiver", type=int, default=9200)
    parser.add_argument("--b-listen", type=int, default=9102)
    parser.add_argument("--b-receiver", type=int, default=9202)
    args = parser.parse_args()

    flows = [
        FlowPath(args.a_listen, args.a_receiver, "A"),
        FlowPath(args.b_listen, args.b_receiver, "B"),
    ]
    by_sock = {f.sock: f for f in flows}
    shared_q: queue.Queue = queue.Queue(maxsize=args.queue_size)
    stop = threading.Event()
    worker = threading.Thread(
        target=drain_queue,
        args=(shared_q, args.service_ms / 1000.0, stop),
        daemon=True,
    )
    worker.start()

    print(
        f"=== Bottleneck relay  queue={args.queue_size}  "
        f"service={args.service_ms}ms ==="
    )
    for f in flows:
        print(f"  Flow {f.name}: listen {f.listen_port} -> receiver {f.receiver[1]}")
    print("Ctrl+C to stop.\n")

    try:
        while True:
            readable, _, _ = select.select([f.sock for f in flows], [], [], 0.5)
            for sock in readable:
                flow = by_sock[sock]
                try:
                    raw, addr = sock.recvfrom(4096)
                except BlockingIOError:
                    continue

                if addr[1] == flow.receiver[1]:
                    if flow.sender_addr:
                        sock.sendto(raw, flow.sender_addr)
                    continue

                flow.sender_addr = addr
                if is_data_plane(raw):
                    try:
                        shared_q.put_nowait((flow, raw))
                    except queue.Full:
                        flow.drops += 1
                        print(
                            f"[DROP] flow {flow.name} queue full "
                            f"(drops={flow.drops})"
                        )
                else:
                    sock.sendto(raw, flow.receiver)
                    flow.forwarded += 1
    except KeyboardInterrupt:
        print("\nStopping relay...")
    finally:
        stop.set()
        for f in flows:
            print(
                f"Flow {f.name}: forwarded={f.forwarded}  "
                f"queue_drops={f.drops}"
            )
            f.sock.close()


if __name__ == "__main__":
    main()
