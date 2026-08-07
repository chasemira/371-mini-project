"""
receiver.py --> server side of our Pipelined Reliable Transfer Protocol

Listens on UDP, accepts a 3-way handshake, then:
  - accepts ONLY the next expected sequence number (Go-Back-N receiver)
  - drops corrupt or out-of-order segments (no ACK for those)
  - writes good payloads, in order, to received_packets.txt
  - ACKs each accepted segment and advertises our window (flow control)
  - closes cleanly on FIN with a FIN-ACK

How to run (start this BEFORE the sender):
    python receiver.py
"""

import os
import random
import socket
import time

from header import PacketHeader

HOST = "127.0.0.1"
RECEIVER_PORT = 9000
SENDER_PORT = 9001       # overwritten with the real peer port when SYN arrives
WINDOW_SIZE = 4          # smaller than sender's 8 on purpose (shows flow control)
MSS = 20                 # must match sender MSS for sequence arithmetic
TIMEOUT = 60             # seconds to wait for the initial SYN


def clear_output(path="received_packets.txt"):
    # delete any leftover output from a previous run so this run starts clean
    if os.path.exists(path):
        os.remove(path)


def log_packet(payload, path="received_packets.txt"):
    # append 1 accepted payload chunk to output file 
    with open(path, "a", encoding="utf-8") as f:
        f.write(payload)


def handshake(sock):
    """
    Receiver side of the 3-way handshake:
      1) Wait for SYN
      2) Reply SYN-ACK with our own random ISN + ack=their_ISN+1
      3) Wait for final ACK that confirms our ISN
    Returns a session dict, or None if something failed.

    Leftover DATA from a previous crashed run is ignored until a real SYN arrives.
    """
    print("[HS] waiting for SYN...")
    sock.settimeout(TIMEOUT)

    # Keep reading until we see a valid SYN (skip stale data from old runs)
    syn = None
    addr = None
    deadline = time.time() + TIMEOUT
    while time.time() < deadline:
        sock.settimeout(max(0.1, deadline - time.time()))
        try:
            raw, addr = sock.recvfrom(4096)
        except socket.timeout:
            break

        cand = PacketHeader.from_bytes(raw)
        if cand.syn and cand.verify_checksum():
            syn = cand
            break
        print(f"[HS] ignore non-SYN (seq={cand.seq_num}) - waiting for SYN...")

    if syn is None:
        print("[HS] no SYN")
        return None

    global SENDER_PORT
    SENDER_PORT = addr[1]
    risn = random.randint(2000, 6000)

    session = {
        "alive": True,
        "peer": addr,
        "local_seq": risn,
        "expected_seq": None,
        "sender_mss": syn.mss,
    }

    # Step 2 — SYN-ACK
    syn_ack = PacketHeader(
        RECEIVER_PORT, SENDER_PORT, risn, syn.seq_num + 1,
        WINDOW_SIZE, MSS, syn=True, ack=True, app_data="SYNACK",
    )
    sock.sendto(syn_ack.to_bytes(), addr)
    print(f"[HS] SYN-ACK seq={risn} ack={syn.seq_num + 1}")

    # Step 3 — wait for final ACK (ignore stray non-ACK packets)
    final = None
    ack_deadline = time.time() + 10
    while time.time() < ack_deadline:
        sock.settimeout(max(0.1, ack_deadline - time.time()))
        try:
            raw, addr2 = sock.recvfrom(4096)
        except socket.timeout:
            break

        cand = PacketHeader.from_bytes(raw)
        if (
            addr2[1] == SENDER_PORT
            and cand.ack
            and not cand.syn
            and not cand.fin
            and cand.ack_num == risn + 1
            and cand.verify_checksum()
        ):
            final = cand
            break
        print(f"[HS] ignore while waiting for final ACK (seq={cand.seq_num})")

    if final is None:
        print("[HS] missing / bad final ACK")
        return None

    session["expected_seq"] = final.seq_num + 1
    session["local_seq"] = risn + 1
    print(f"[HS] established  expect_seq={session['expected_seq']}")
    return session


def send_ack(sock, session, acked_seq):
    #tell sender we accepted 'acked_seq' --> window_size in ACK is our advertised rwnd (flow control signal)
    ack = PacketHeader(
        RECEIVER_PORT, SENDER_PORT, session["local_seq"], acked_seq,
        WINDOW_SIZE, MSS, ack=True,
    )
    sock.sendto(ack.to_bytes(), session["peer"])
    session["local_seq"] += 1
    print(f"[ACK] for seq={acked_seq}  rwnd={WINDOW_SIZE}")


def receive_data(sock, session):
    """
    In-order data loop (classic Go-Back-N receiver):

      - If FIN: stop and let finish() run
      - If checksum fails: DROP (no ACK) — sender will time out and retransmit
      - If seq != expected: DROP as out-of-order (no ACK)
      -  Else: write payload, ACK it, advance expected by MSS

    returning True means we saw FIN; False means we idled out.
    """
    expected = session["expected_seq"]
    sock.settimeout(30)  # idle gap long enough for sender RTOs under loss

    while True:
        try:
            raw, addr = sock.recvfrom(4096)
        except socket.timeout:
            print("[DATA] idle timeout")
            return False
        except ConnectionResetError:
            # ignore Windows ICMP reset noise and keep listening
            continue

        # only accept packets from the peer we shook hands with
        if addr[1] != SENDER_PORT:
            continue

        pkt = PacketHeader.from_bytes(raw)

        # teardown request from sender
        if pkt.fin:
            print("[FIN] received")
            return True

        # integrity check --> reject flipped / damaged segments
        if not pkt.verify_checksum():
            print(f"[DROP] corrupt seq={pkt.seq_num}")
            continue

        # ordering check — reject anything that is not the next byte/seq we need
        if pkt.seq_num != expected:
            print(f"[DROP] out-of-order seq={pkt.seq_num} expected={expected}")
            continue

        # good packet: deliver to "app" (file) and acknowledge
        log_packet(pkt.app_data)
        print(f"[RECV] seq={pkt.seq_num}  '{pkt.app_data[:30]}'")
        send_ack(sock, session, pkt.seq_num)

        # advance the next expected sequence number by one MSS
        expected += session["sender_mss"]
        session["expected_seq"] = expected


def finish(sock, session):
    #reply to FIN with a combined FIN-ACK, then wait briefly for sender's final ACK before we close our socket
    fin_ack = PacketHeader(
        RECEIVER_PORT, SENDER_PORT, session["local_seq"], session["expected_seq"],
        WINDOW_SIZE, MSS, fin=True, ack=True, app_data="FINACK",
    )
    sock.sendto(fin_ack.to_bytes(), session["peer"])
    print("[FIN] sent FIN-ACK")

    sock.settimeout(5)
    try:
        raw, addr = sock.recvfrom(4096)
        last = PacketHeader.from_bytes(raw)
        if last.ack:
            print("[FIN] final ACK - closed")
    except (socket.timeout, ConnectionResetError):
        # final ACK is optional for us to exit cleanly in this demo
        print("[FIN] closed (no final ACK)")


def main():
    """Top-level: listen -> handshake -> receive file -> teardown."""
    clear_output()

    # UDP socket --> same unreliable channel the sender uses
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((HOST, RECEIVER_PORT))
    print(f"=== Receiver on {HOST}:{RECEIVER_PORT} ===")

    session = handshake(sock)
    if not session:
        sock.close()
        raise SystemExit("Handshake failed")

    # only run FIN exchange if the data loop ended because of FIN
    if receive_data(sock, session):
        finish(sock, session)

    sock.close()
    print("Wrote received_packets.txt")


if __name__ == "__main__":
    main()
