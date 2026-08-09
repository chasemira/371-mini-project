# Fairness bonus test

Two PRTP flows share a finite bottleneck queue, then we measure Jain's fairness index.

## Layout

```text
fairness-test/
  bottleneck_relay.py   # shared queue
  run_fairness.py       # launches 2 flows + relay
  plot_fairness.py      # plots throughput + Jain
  data_b.txt            # Flow B payload (same size as ../data.txt)
  results/              # csv + received txt
  pictures/             # png plots
```

## Run (from project root)

```bash
python fairness-test/run_fairness.py
python fairness-test/run_fairness.py --trials 5
python fairness-test/plot_fairness.py
```

## Topology

- Flow A: sender `9001` → relay `9100` → receiver `9200`
- Flow B: sender `9003` → relay `9102` → receiver `9202`
