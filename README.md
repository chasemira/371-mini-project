# CMPT 371 Mini-Project — Option (2)

Pipelined Reliable Transfer Protocol over UDP.

## Files (keep it simple)

| File | Purpose |
|------|---------|
| `header.py` | Packet header + checksum |
| `sender.py` | Handshake, send, retransmit, congestion control, CWND plot |
| `receiver.py` | Handshake, receive, ACK, write output |
| `data.txt` | Payload to send |
| `received_packets.txt` | Reconstructed payload (created at runtime) |
| `REPORT.md` | Full report draft → export to PDF for Canvas |

## Run

```bash
# Terminal 1
python receiver.py

# Terminal 2
python sender.py
```

When finished: check `received_packets.txt` and `cwnd_growth.png`.

## Experiments

Edit knobs at the top of `sender.py` / `receiver.py`:

- `LOSS_PROBABILITY` — `0.0` clean CWND curve, `0.35` stress test
- `WINDOW_SIZE` — flow control
- `MSS` — segment size

## Wireshark

Loopback interface, filter: `udp.port == 9000 or udp.port == 9001`
