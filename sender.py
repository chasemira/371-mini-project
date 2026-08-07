"""
sender.py — client side of our Pipelined Reliable Transfer Protocol.

Runs on top of UDP. UDP does NOT give reliability, so we add:
  1) 3-way handshake to open a connection
  2) Go-Back-N pipelining + retransmit on timeout
  3) Flow control (respect receiver's advertised window)
  4) Congestion control (TCP Tahoe-style CWND)
  5) Loss/corruption simulation in udt_send() for testing
  6) FIN teardown to close cleanly
  7) CWND plot for the report

How to run (start receiver first):
    python sender.py
"""

import copy
import random
import socket
import time

import matplotlib.pyplot as plt

from header import PacketHeader

HOST = "127.0.0.1"       # loopback so both sides run on this machine
SENDER_PORT = 9001       # our local UDP bind port
RECEIVER_PORT = 9000     # peer's UDP port
WINDOW_SIZE = 8          # max segments WE are willing to have in flight
MSS = 20                 # bytes of app data per segment (small => many packets)
LOSS_PROBABILITY = 0.35  # chance each DATA packet is dropped OR corrupted
                         # set to 0.0 for a clean CWND "no loss" graph
MAX_RETRIES = 8          # how many times to retry FIN if no FIN-ACK
ALPHA = 0.125            # weight for EstimatedRTT (Jacobson/Karels)
BETA = 0.25              # weight for DevRTT
INITIAL_TIMEOUT = 1.5    # seconds, used before we have an RTT sample

# congestion-control state (bytes, like TCP)
CWND = MSS                                 # start cautious: 1 MSS
SSTHRESH = (WINDOW_SIZE * MSS) // 2        # slow-start threshold
CWND_MAX = WINDOW_SIZE * MSS               # never exceed sender window in bytes
SLOW_START = "SLOW_START"
CONGESTION_AVOIDANCE = "CONGESTION_AVOIDANCE"
congestion_state = SLOW_START

# samples collected so we can plot CWND vs time at the end
cwnd_values = []
cwnd_times = []


def read_data(path="data.txt"):
    #load  application message we will fragment and send
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def log_cwnd(event=""):
    # record one CWND sample for the CSV log and the final plot
    cwnd_values.append(CWND)
    cwnd_times.append(time.time())
    with open("cwnd_log.csv", "a", encoding="utf-8") as f:
        f.write(f"{time.time()},{CWND},{SSTHRESH},{congestion_state},{event}\n")


def prepare_packets(data):
    # split  full text into MSS-sized chunks (one chunk = one data segment
    return [data[i : i + MSS] for i in range(0, len(data), MSS)]


def udt_send(sock, packet, peer):
    """
    Unreliable datagram send (the Module 3 "udt_send" idea).

    Reliability lives ABOVE this function. Here we optionally:
      - drop the packet (never call sendto), OR
      - corrupt it (flip checksum by +1) so the receiver will reject it.

    Control packets (any segment with SYN / ACK / FIN set) are left alone
    so handshake and teardown stay reliable. Only pure DATA segments
    (no control flags) are candidates for loss/corruption.
    """
    # IMPORTANT: final handshake ACK has ack=True AND a short app_data string.
    # We must still treat it as control — otherwise loss can break the handshake.
    is_control = packet.syn or packet.ack or packet.fin

    if (not is_control) and random.random() < LOSS_PROBABILITY:
        if random.random() < 0.5:
            # silent loss: pretend a router dropped it
            print(f"  [LOSS] dropped seq={packet.seq_num}")
            return "dropped"

        # corruption: deliver bytes, but make checksum wrong
        bad = PacketHeader.from_bytes(packet.to_bytes())
        bad.checksum = (bad.checksum + 1) & 0xFFFF
        sock.sendto(bad.to_bytes(), peer)
        print(f"  [CORRUPT] seq={packet.seq_num}")
        return "corrupted"

    # normal path: send the correct packet over UDP
    sock.sendto(packet.to_bytes(), peer)
    return "sent"


def handshake(sock):
    """
    3-way handshake (why 3 steps? so both sides confirm each other's ISN):
      1) We send SYN with a random initial sequence number (ISN)
      2) Receiver replies SYN-ACK (its ISN + ack = our ISN+1)
      3) We send final ACK (ack = their ISN+1)
    Returns a connection dict; conn['alive'] is True if it worked.
    """
    conn = {
        "alive": False,
        "receiver_window": WINDOW_SIZE,
        "receiver_mss": MSS,
        "receiver_seq": 0,
        "sender_seq": 0,
        "sender_ack": 0,
    }

    # step 1 — SYN
    isn = random.randint(1000, 5000)
    syn = PacketHeader(
        SENDER_PORT, RECEIVER_PORT, isn, 0, WINDOW_SIZE, MSS,
        syn=True, app_data="CONNECT",
    )
    print(f"[HS] SYN seq={isn}")
    udt_send(sock, syn, (HOST, RECEIVER_PORT))
    sock.settimeout(INITIAL_TIMEOUT)

    # step 2 — wait for SYN-ACK
    try:
        raw, addr = sock.recvfrom(4096)
    except socket.timeout:
        print("[HS] timeout waiting for SYN-ACK")
        return conn

    reply = PacketHeader.from_bytes(raw)
    # must be SYN+ACK, acknowledge our ISN, and pass checksum
    if not (reply.syn and reply.ack and reply.ack_num == isn + 1 and reply.verify_checksum()):
        print("[HS] bad SYN-ACK")
        return conn

    # remember what the receiver advertised (window / MSS / its seq)
    conn["receiver_window"] = reply.window_size
    conn["receiver_mss"] = reply.mss
    conn["receiver_seq"] = reply.seq_num
    conn["sender_seq"] = isn + 1
    conn["sender_ack"] = reply.seq_num + 1

    # step 3 — final ACK
    ack = PacketHeader(
        SENDER_PORT, RECEIVER_PORT, conn["sender_seq"], conn["sender_ack"],
        WINDOW_SIZE, MSS, ack=True, app_data="ESTABLISHED",
    )
    udt_send(sock, ack, (HOST, RECEIVER_PORT))
    conn["sender_seq"] += 1   # next number is for the first DATA segment
    conn["alive"] = True
    print(f"[HS] done  first_data_seq={conn['sender_seq']} rwnd={conn['receiver_window']}")
    return conn


def effective_window(receiver_rwnd):
    """
    How many DATA segments may be in flight right now?

    Takes the MIN of:
      - our configured WINDOW_SIZE (sender-side cap)
      - floor(CWND / MSS)          (congestion control)
      - receiver_rwnd              (flow control — what they can accept)

    This is what makes both flow control and congestion control real.
    """
    from_cwnd = max(1, CWND // MSS)
    return max(1, min(WINDOW_SIZE, from_cwnd, receiver_rwnd))


def on_ack():
    #grow CWND after a successful ACK (Tahoe-style slow start / CA)
    global CWND, congestion_state

    if congestion_state == SLOW_START:
        if CWND < SSTHRESH:
            # exponential growth: double each ACK (simplified demo)
            CWND = min(CWND * 2, CWND_MAX)
        else:
            # crossed the threshold — switch to linear growth
            congestion_state = CONGESTION_AVOIDANCE
            CWND = min(CWND + MSS, CWND_MAX)
    else:
        # congestion avoidance: add about 1 MSS per ACK
        CWND = min(CWND + MSS, CWND_MAX)

    log_cwnd("acked")


def on_timeout():
    """
    Packet loss inferred from timeout (Tahoe response):
      - cut ssthresh to half of current CWND (at least 2 MSS)
      - reset CWND to 1 MSS
      - go back to slow start
    """
    global CWND, SSTHRESH, congestion_state
    SSTHRESH = max(CWND // 2, 2 * MSS)
    CWND = MSS
    congestion_state = SLOW_START
    log_cwnd("timeout")


def send_data(sock, conn, chunks):
    """
    Go-Back-N data transfer with flow + congestion control.

    Ideas:
      - 'base'      = oldest unACKed sequence number (left edge of window)
      - 'next_seq'  = next new sequence number we will send
      - fill the window up to effective_window(), then wait for ACK of 'base'
      - on ACK: slide base forward, grow CWND, update RTO from SampleRTT
      - on timeout: cut CWND, retransmit from base (Go-Back-N)
    """
    global CWND, congestion_state

    # start a fresh CSV log for this run
    with open("cwnd_log.csv", "w", encoding="utf-8") as f:
        f.write("time,cwnd,ssthresh,phase,event\n")
    log_cwnd("init")

    first = conn["sender_seq"]
    last = first + MSS * (len(chunks) - 1)  # seq of the final chunk
    base = first
    next_seq = first
    outstanding = {}   # seq -> [send_time, conn_snapshot, payload]

    # RTT estimator state (starts empty until first SampleRTT)
    estimated_rtt = None
    dev_rtt = 0.0
    rto = INITIAL_TIMEOUT

    print(f"[DATA] {len(chunks)} segments  seq {first}..{last}")

    while base <= last:
        # 1) fill the pipeline up to the effective window 
        win = effective_window(conn["receiver_window"])
        limit = base + win * MSS

        while next_seq <= last and next_seq < limit:
            idx = (next_seq - first) // MSS
            conn["sender_seq"] = next_seq
            pkt = PacketHeader(
                SENDER_PORT, RECEIVER_PORT, next_seq, conn["sender_ack"],
                WINDOW_SIZE, MSS, app_data=chunks[idx],
            )
            outcome = udt_send(sock, pkt, (HOST, RECEIVER_PORT))
            # remember what we sent so we can retransmit later if needed
            outstanding[next_seq] = [time.time(), copy.deepcopy(conn), chunks[idx]]
            print(f"[SEND] seq={next_seq} {outcome}  cwnd={CWND} win={win}")
            next_seq += MSS

        # 2) Wait for ACK of the window base (or time out) 
        sock.settimeout(rto)
        ack = None
        try:
            deadline = time.time() + rto
            while time.time() < deadline:
                sock.settimeout(max(0.05, deadline - time.time()))
                try:
                    raw, addr = sock.recvfrom(4096)
                except ConnectionResetError:
                    # windows quirk after ICMP port-unreachable; ignore and keep waiting
                    continue

                cand = PacketHeader.from_bytes(raw)
                if addr[1] != RECEIVER_PORT or not cand.verify_checksum() or not cand.ack:
                    continue
                # our receiver ACKs with ack_num = the seq it just accepted
                if cand.ack_num == base:
                    ack = cand
                    break
                print(f"[ACK] ignore ack={cand.ack_num} want={base}")
        except socket.timeout:
            ack = None

        # 3a) timeout path: congestion response + Go-Back-N retransmit 
        if ack is None:
            print(f"[RTO] base={base} rto={rto:.2f}s")
            on_timeout()
            rto = min(8.0, rto * 2)  #exponential backoff of the timeout

            end = min(base + effective_window(conn["receiver_window"]) * MSS, next_seq)
            for seq in range(base, end, MSS):
                if seq not in outstanding:
                    continue
                conn["sender_seq"] = seq
                pkt = PacketHeader(
                    SENDER_PORT, RECEIVER_PORT, seq, conn["sender_ack"],
                    WINDOW_SIZE, MSS, app_data=outstanding[seq][2],
                )
                outcome = udt_send(sock, pkt, (HOST, RECEIVER_PORT))
                outstanding[seq][0] = time.time()  # reset send timestamp
                print(f"[REXMIT] seq={seq} {outcome}")
            continue

        #3b) good ACK path: update RTT, slide window, grow CWND 
        sample = time.time() - outstanding[base][0]  # SampleRTT
        if estimated_rtt is None:
            # First sample initializes the estimator
            estimated_rtt = sample
            dev_rtt = sample / 2
        else:
            err = abs(sample - estimated_rtt)
            estimated_rtt = (1 - ALPHA) * estimated_rtt + ALPHA * sample
            dev_rtt = (1 - BETA) * dev_rtt + BETA * err
        # classic RTO formula, clamped so it stays practical on loopback
        rto = max(0.4, min(8.0, estimated_rtt + 4 * dev_rtt))

        #receiver may change its advertised window (flow control)
        conn["receiver_window"] = max(1, ack.window_size)
        on_ack()
        print(f"[ACK] seq={base} rtt={sample:.3f}s cwnd={CWND} phase={congestion_state}")
        del outstanding[base]
        base += MSS  # slide the left edge of the window forward

    conn["sender_seq"] = last + MSS
    print("[DATA] transfer complete")


def terminate(sock, conn):
    #graceful close:Sender FIN  ->  Receiver FIN-ACK  ->  Sender final ACK
    fin = PacketHeader(
        SENDER_PORT, RECEIVER_PORT, conn["sender_seq"], conn["sender_ack"],
        WINDOW_SIZE, MSS, fin=True, app_data="BYE",
    )
    print("[FIN] sending FIN")
    udt_send(sock, fin, (HOST, RECEIVER_PORT))
    sock.settimeout(INITIAL_TIMEOUT)

    for _ in range(MAX_RETRIES):
        try:
            raw, addr = sock.recvfrom(4096)
        except (socket.timeout, ConnectionResetError):
            # no FIN-ACK yet — resend FIN and try again
            udt_send(sock, fin, (HOST, RECEIVER_PORT))
            continue

        reply = PacketHeader.from_bytes(raw)
        if addr[1] == RECEIVER_PORT and reply.fin and reply.ack and reply.verify_checksum():
            # final ACK confirms we saw their FIN-ACK
            final = PacketHeader(
                SENDER_PORT, RECEIVER_PORT, conn["sender_seq"] + 1, reply.seq_num + 1,
                WINDOW_SIZE, MSS, ack=True, app_data="CLOSED",
            )
            udt_send(sock, final, (HOST, RECEIVER_PORT))
            print("[FIN] closed")
            return

    print("[FIN] gave up after retries")


def plot_cwnd():
    # save (and try to show) CWND vs time for the report
    if len(cwnd_times) < 2:
        return
    t0 = cwnd_times[0]
    xs = [t - t0 for t in cwnd_times]
    plt.figure(figsize=(9, 4.5))
    plt.plot(xs, cwnd_values, marker="o", markersize=4)
    plt.xlabel("Time (s)")
    plt.ylabel("CWND (bytes)")
    plt.title("Congestion Window over Time")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("cwnd_growth.png", dpi=150)
    print("Saved cwnd_growth.png")
    try:
        plt.show()
    except Exception:
        pass


def main():
    # top-level: open connection -> send file -> close -> plot
    data = read_data("data.txt")
    chunks = prepare_packets(data)
    if not chunks:
        raise SystemExit("data.txt is empty")

    # UDP socket (SOCK_DGRAM) — unreliable by design; we add reliability above it
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, SENDER_PORT))

    print(f"=== Sender {HOST}:{SENDER_PORT} -> {RECEIVER_PORT}  loss={LOSS_PROBABILITY} ===")
    conn = handshake(sock)
    if not conn["alive"]:
        sock.close()
        raise SystemExit("Handshake failed")

    send_data(sock, conn, chunks)
    terminate(sock, conn)
    sock.close()
    plot_cwnd()


if __name__ == "__main__":
    main()
