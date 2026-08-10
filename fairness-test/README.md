# Fairness bonus test

Two PRTP flows share a finite bottleneck queue, then we measure Jain's fairness index.

## Layout

```text
fairness-test/
  bottleneck_relay.py   # shared queue
  run_fairness.py       # launches 2 flows + relay
  plot_fairness.py      # plots throughput + Jain
  data_b.txt            # short Flow B payload (smoke tests)
  data_long_a.txt       # ~4 KB Flow A payload (default trials)
  data_long_b.txt       # ~4 KB Flow B payload (default trials)
  results/              # csv + received txt
  pictures/             # png plots
```

Default payloads are ~4096 bytes (~205 MSS=20 segments) so both congestion
windows compete past startup. Use `--short` for the original 289-byte files.

## Run (from project root)

```bash
python fairness-test/run_fairness.py --trials 5
python fairness-test/plot_fairness.py
```

Quick smoke test:

```bash
python fairness-test/run_fairness.py --short
```

After multi-trial runs, `results/fairness_summary.csv` has per-trial throughputs
and Jain index; the launcher also prints mean / min / max Jain.

Latest 5-trial long-payload run (4096 B each flow, shared queue):

| Trial | Flow A (B/s) | Flow B (B/s) | Jain |
|------:|-------------:|-------------:|-----:|
| 1 | 48.51 | 49.25 | 1.000 |
| 2 | 49.37 | 48.56 | 1.000 |
| 3 | 49.01 | 49.13 | 1.000 |
| 4 | 48.59 | 49.37 | 1.000 |
| 5 | 49.01 | 49.09 | 1.000 |

- Mean Jain: **1.000**
- Min Jain: **1.000**
- Max Jain: **1.000**

Figures: `pictures/fairness_throughput.png`, `pictures/fairness_trials.png`.

## Topology

- Flow A: sender `9001` → relay `9100` → receiver `9200`
- Flow B: sender `9003` → relay `9102` → receiver `9202`
