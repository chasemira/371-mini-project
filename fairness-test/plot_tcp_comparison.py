"""
plot_tcp_comparison.py — Figure for §7.4, the TCP coexistence experiment.

Two panels rather than one, because throughput at 0% loss (~800 B/s) and at
35% loss (~15 B/s) differ by roughly 50x. Sharing one axis would flatten the
lossy panel to invisibility, and a second y-axis on one plot would be worse.

Error bars show the observed min-max range across the five trials, not a
standard error. That is deliberate: the point of the right-hand panel is that
the ranges overlap, so the apparent slowdown at 35% loss is not established.

Usage:
    ./venv/bin/python plot_tcp_comparison.py
"""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PAYLOAD = 289  # bytes

# (label, alone(avg,min,max), with_load(avg,min,max)) — seconds, from tcp_comparison.py
PANELS = [
    ("No injected loss (0%)",   (0.36, 0.34, 0.41),   (0.39, 0.35, 0.40)),
    ("Injected loss (35%)",     (16.04, 11.31, 28.48), (23.40, 14.81, 45.26)),
]

ALONE = "#2a78d6"      # categorical slot 1
WITH_LOAD = "#eb6834"  # categorical slot 2
INK = "#0b0b0b"
MUTED = "#52514e"


def throughput(seconds):
    """Seconds -> B/s. Shorter time is higher throughput, so min/max swap."""
    avg, lo, hi = seconds
    return PAYLOAD / avg, PAYLOAD / hi, PAYLOAD / lo


def main():
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.2))

    for ax, (title, alone_s, load_s) in zip(axes, PANELS):
        series = [("Protocol alone", throughput(alone_s), ALONE),
                  ("Protocol + TCP load", throughput(load_s), WITH_LOAD)]

        for x, (label, (avg, lo, hi), colour) in enumerate(series):
            ax.bar(x, avg, width=0.55, color=colour, label=label, zorder=3)
            # observed range across the five trials
            ax.errorbar(x, avg, yerr=[[avg - lo], [hi - avg]], fmt="none",
                        ecolor=MUTED, elinewidth=1.2, capsize=5, zorder=4)
            ax.text(x, hi, f"{avg:.1f}", ha="center", va="bottom",
                    fontsize=10, color=INK, zorder=5)

        ax.set_title(title, fontsize=11, color=INK, pad=10)
        ax.set_xticks([0, 1])
        ax.set_xticklabels(["Alone", "+ TCP load"], fontsize=9, color=MUTED)
        ax.set_ylabel("Throughput (B/s)", fontsize=9, color=MUTED)
        ax.grid(axis="y", alpha=0.25, zorder=0)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.margins(y=0.18)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", ncol=2, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, -0.02))
    fig.suptitle("Protocol throughput with and without competing TCP traffic",
                 fontsize=12, color=INK)
    fig.tight_layout(rect=(0, 0.05, 1, 0.95))
    fig.savefig("fig17_tcp_comparison.png", dpi=200)
    print("wrote fig17_tcp_comparison.png")


if __name__ == "__main__":
    main()
