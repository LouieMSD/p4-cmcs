#!/usr/bin/env python3
"""
plot_results.py
===============
Read the per-VN throughput CSV files produced by run_test.py and generate
a multi-VN throughput time-series line chart.

Standalone usage:
    python3 test/plot_results.py

Integrated usage:
    Set AUTO_PLOT = True in run_test.py; the script is invoked automatically
    at the end of each experiment.

Output files (written to RESULT_DIR):
    throughput.pdf   Vector format; recommended for publication figures.
    throughput.png   300 dpi raster; for preview use.
"""

import os
import sys
import csv

# Ensure that run_test (located in the same test/ directory) can be imported
# regardless of the working directory from which this script is invoked.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

from run_test import (
    VNS,
    RESULT_DIR,
    TRAFFIC_WARMUP_SKIP,
    TRAFFIC_COOLDOWN_SKIP,
    TOTAL_BW_C,
    _derive_phases,
)

import matplotlib
matplotlib.use('Agg')
# Embed TrueType fonts in PDF/PS output to avoid font substitution issues.
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype']  = 42

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator


# ═══════════════════════════════════════════════════════════════════════════════
#  ① Style parameters
# ═══════════════════════════════════════════════════════════════════════════════

# Per-VN line styles using the Okabe-Ito colour-blind-safe palette.
# Each entry: (colour, linestyle, marker).
# Colour and linestyle together provide dual encoding so the chart remains
# legible when printed in greyscale or viewed by colour-blind readers.
VN_STYLES = {
    1: ('#0072B2', '-',   None),   # Blue         solid
    2: ('#E69F00', '--',  None),   # Orange       dashed
    3: ('#009E73', '-.',  None),   # Green        dash-dot
    4: ('#CC79A7', ':',   None),   # Violet-pink  dotted
    5: ('#56B4E9', '-',   None),   # Sky-blue     solid
    6: ('#D55E00', '--',  None),   # Vermillion   dashed
}

FIGSIZE   = (10, 4)
FONTSIZE  = 9
LINEWIDTH = 1.8

# Z-order constants — higher values appear on top.
Z_BG       = 1   # Background colour fills (axvspan).
Z_GRID     = 2   # Horizontal grid lines (axhline).
Z_BOUNDARY = 3   # Phase-boundary vertical lines (axvline).
Z_DATA     = 5   # Measured throughput line plots.
Z_LABEL    = 6   # Phase name text annotations.


# ═══════════════════════════════════════════════════════════════════════════════
#  ② CSV loading
# ═══════════════════════════════════════════════════════════════════════════════

def _load_csv():
    """
    Load per-VN throughput time series from CSV files.

    Priority:
      1. vn{vnid}_full.csv  — complete timeline including zero-traffic intervals
         (written by _write_full_csv in run_test.py).
      2. vn{vnid}_throughput.csv  — fallback containing only analysis-window
         samples (written by _write_csv in run_test.py).

    The full CSV is preferred because it preserves zero-throughput intervals
    between sessions, so the plot accurately shows idle periods rather than
    connecting non-adjacent data points across inactive gaps.

    Returns:
        dict {vnid: [(t_start_sec, throughput_mbps), ...]} sorted by time.
    """
    data = {}
    for vnid in VNS:
        full_path   = os.path.join(RESULT_DIR, 'vn{}_full.csv'.format(vnid))
        window_path = os.path.join(RESULT_DIR, 'vn{}_throughput.csv'.format(vnid))

        if os.path.exists(full_path):
            chosen = full_path
        elif os.path.exists(window_path):
            print('[WARN] Full CSV not found; falling back to analysis-window '
                  'data: {}'.format(window_path))
            chosen = window_path
        else:
            print('[WARN] No CSV found for vnid={}'.format(vnid))
            data[vnid] = []
            continue

        rows = []
        with open(chosen, 'r') as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                t    = float(row['t_start_sec'])
                mbps = float(row['throughput_mbps'])
                rows.append((t, mbps))
        rows.sort(key=lambda x: x[0])
        data[vnid] = rows

    return data


# ═══════════════════════════════════════════════════════════════════════════════
#  ③ Main plotting function
# ═══════════════════════════════════════════════════════════════════════════════

def plot_results():
    """
    Generate the multi-VN throughput time-series chart and write it to
    RESULT_DIR/throughput.pdf and RESULT_DIR/throughput.png.

    Chart structure:
      - X axis: time in seconds relative to client t=0.
      - Y axis: throughput in Mbps (L2, includes Ethernet header).
      - One line per VN, styled with a unique colour and linestyle combination.
      - Vertical dashed lines mark phase boundaries (session start/end times).
      - Phase name labels are placed at the top of the chart when more than
        one non-idle phase exists.
      - Each VN's line is drawn only over its active session intervals to avoid
        misleading connections across inactive periods.
      - Horizontal grid lines are drawn with axhline rather than ax.grid()
        so they remain visible above axvspan background fills.
    """

    phases  = _derive_phases()
    ts_data = _load_csv()

    t_total_start = min(float(sess['start'])
                        for vn in VNS.values()
                        for sess in vn['sessions'])
    t_total_end   = max(float(sess['end'])
                        for vn in VNS.values()
                        for sess in vn['sessions'])

    # ── Canvas setup ──────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=FIGSIZE)

    ax.set_xlim(t_total_start, t_total_end)
    ylim_top = TOTAL_BW_C * 1.15
    ax.set_ylim(0, ylim_top)

    ax.set_xlabel('Time (s)',          fontsize=FONTSIZE)
    ax.set_ylabel('Throughput (Mbps)', fontsize=FONTSIZE)
    ax.tick_params(labelsize=FONTSIZE - 1)

    ax.xaxis.set_major_locator(MultipleLocator(5))
    ax.yaxis.set_major_locator(MultipleLocator(1))
    ax.yaxis.set_minor_locator(MultipleLocator(0.5))
    ax.tick_params(axis='y', which='major', length=4, width=0.8)
    ax.tick_params(axis='y', which='minor', length=2, width=0.6)

    # ── Horizontal grid lines ─────────────────────────────────────────────────
    # Drawn with axhline (Z_GRID=2) rather than ax.grid() so that they appear
    # above axvspan background fills instead of being hidden beneath them.
    y_major_vals = [round(i * 0.5, 1)
                    for i in range(int(ylim_top / 0.5) + 2)
                    if round(i * 0.5, 1) <= ylim_top]

    for y in y_major_vals:
        ax.axhline(y, color='#BBBBBB', linewidth=0.5,
                   linestyle='--', zorder=Z_GRID, alpha=0.8)

    # ── Phase-boundary vertical lines ─────────────────────────────────────────
    boundaries = sorted(
        {ph['start'] for ph in phases} | {ph['end'] for ph in phases}
    )
    for b in boundaries:
        ax.axvline(x=b, color='#999999', linestyle='--',
                   linewidth=0.9, zorder=Z_BOUNDARY)

    # ── Phase name labels ─────────────────────────────────────────────────────
    # Shown only when more than one non-idle phase exists.
    non_idle_phases = [ph for ph in phases if not ph['is_idle']]
    if len(non_idle_phases) > 1:
        label_y = ylim_top * 0.984
        for ph in phases:
            mid_x = (ph['start'] + ph['end']) / 2.0
            label = ph['name'].replace('Phase_', '').replace('-', '–')
            ax.text(mid_x, label_y, label,
                    ha='center', va='top',
                    fontsize=FONTSIZE - 2, color='#555555',
                    clip_on=True, zorder=Z_LABEL)

    # ── Per-VN throughput lines ───────────────────────────────────────────────
    # Each VN is plotted only over its active session intervals.  A tolerance
    # of 0.6 s is added to each interval's right edge to include the last 1-s
    # bin whose left boundary may equal sess.end exactly due to floating-point
    # rounding in tshark output.
    legend_handles = []

    for vnid, vn in sorted(VNS.items()):
        color, ls, _ = VN_STYLES.get(vnid, ('#333333', '-', None))

        proto_label = '/'.join(sorted({sess['proto']
                                       for sess in vn['sessions']}))

        rows = ts_data.get(vnid, [])

        if not rows:
            legend_handles.append(
                Line2D([0], [0],
                       color=color, linestyle=ls,
                       linewidth=LINEWIDTH,
                       label='VN {} ({}) — no data'.format(vnid, proto_label)))
            continue

        active_intervals = sorted(
            [(float(sess['start']), float(sess['end']))
             for sess in vn['sessions']],
            key=lambda x: x[0])

        for (a_start, a_end) in active_intervals:
            seg = [(t, m) for (t, m) in rows
                   if a_start <= t <= a_end + 0.6]
            if not seg:
                continue
            ax.plot([p[0] for p in seg], [p[1] for p in seg],
                    color=color, linestyle=ls,
                    linewidth=LINEWIDTH,
                    zorder=Z_DATA, label=None)

        legend_handles.append(
            Line2D([0], [0],
                   color=color, linestyle=ls,
                   linewidth=LINEWIDTH,
                   label='VN {} ({})'.format(vnid, proto_label)))

    # ── Legend ────────────────────────────────────────────────────────────────
    ax.legend(
        handles=legend_handles,
        bbox_to_anchor=(0.5, 1.02),
        loc='lower center',
        ncol=len(legend_handles),
        fontsize=FONTSIZE - 1,
        facecolor='white',
        framealpha=1.0,
        edgecolor='#BBBBBB',
    )

    fig.subplots_adjust(top=0.88)

    # ── Save output ───────────────────────────────────────────────────────────
    os.makedirs(RESULT_DIR, exist_ok=True)
    pdf_path = os.path.join(RESULT_DIR, 'throughput.pdf')
    png_path = os.path.join(RESULT_DIR, 'throughput.png')

    fig.savefig(pdf_path, dpi=300, bbox_inches='tight')
    print('[PLOT] wrote -> {}'.format(pdf_path))

    fig.savefig(png_path, dpi=300, bbox_inches='tight')
    print('[PLOT] wrote -> {}'.format(png_path))

    plt.close(fig)


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    plot_results()