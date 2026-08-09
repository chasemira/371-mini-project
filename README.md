# CMPT 371 Mini-Project — Option (2)

Pipelined Reliable Transfer Protocol over UDP.

## Layout

| Path | Purpose |
|------|---------|
| `header.py` | Packet header + checksum |
| `sender.py` | Handshake, send, retransmit, congestion control, CWND plot |
| `receiver.py` | Handshake, receive, ACK, write output |
| `data.txt` | Payload to send |
| `results/` | Run outputs (received text, CWND CSV logs) |
| `figures/` | Report diagrams + Wireshark screenshots |
| `captures/` | Wireshark `.pcapng` captures |
| `fairness-test/` | Bonus fairness experiment (scripts, results, pictures) |

## Run (main protocol)

```bash
# Terminal 1
python receiver.py

# Terminal 2
python sender.py
```

When finished: check `results/received_packets.txt` and `figures/diagrams/cwnd_growth.png`.

## Experiments

Edit knobs at the top of `sender.py` / `receiver.py`:

- `LOSS_PROBABILITY` — `0.0` clean CWND curve, `0.35` stress test
- `WINDOW_SIZE` — flow control
- `MSS` — segment size

## Wireshark

Loopback interface, filter: `udp.port == 9000 or udp.port == 9001`

Captures live in `captures/`.

## Bonus: Fairness

```bash
python fairness-test/run_fairness.py
python fairness-test/run_fairness.py --trials 5
python fairness-test/plot_fairness.py
```

See `fairness-test/README.md` for details. Outputs land in `fairness-test/results/` and `fairness-test/pictures/`.
