#!/usr/bin/env python3
"""
analyze_results.py
==================
Read the per-VN throughput CSV files produced by run_test.py, compute the
mean throughput and its deviation from the ideal bandwidth allocation for each
VN in each experiment phase, and write the results as terminal tables and
Markdown files.

Standalone usage:
    python3 test/analyze_results.py

Integrated usage:
    Set AUTO_ANALYZE = True in run_test.py; the script is invoked automatically
    at the end of each experiment.

Dependencies:
    pip3 install tabulate --break-system-packages

Output files (written to RESULT_DIR):
    table_config.md   VN configuration table (guaranteed bandwidth, weight,
                      protocol, injection rate, ideal allocation per phase)
    table_error.md    Error analysis table (mean, standard deviation,
                      absolute error, relative error per VN per phase)
    table_note.md     Both tables combined in a single Markdown file
"""

import os
import sys
import csv
import statistics

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
    MIN_ANALYSIS_WINDOW,
    _derive_phases,
    _parse_bw_mbps,
)

try:
    from tabulate import tabulate
except ImportError:
    sys.exit(
        '[ERROR] tabulate is not installed.\n'
        'Run: pip3 install tabulate --break-system-packages'
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  Ideal bandwidth calculation
# ═══════════════════════════════════════════════════════════════════════════════

def _get_demands(flow_map):
    """
    Extract the effective traffic demand D_i (Mbps) for each active VN in a
    phase.

    For UDP sessions, D_i is the configured udp_bw rate (a known, constant
    injection rate).  For TCP greedy sessions, D_i is set to TOTAL_BW_C
    because TCP senders attempt to consume all available bandwidth, making the
    actual demand equal to the full link capacity for allocation purposes.

    Args:
        flow_map : dict {vnid: session_dict} for the current phase.

    Returns:
        dict {vnid: float} of effective demands in Mbps.
    """
    demands = {}
    for vnid, sess in flow_map.items():
        if sess['proto'] == 'UDP':
            demands[vnid] = _parse_bw_mbps(sess['udp_bw'])
        else:
            demands[vnid] = TOTAL_BW_C
    return demands


def _compute_ideal(active_vn_ids, demands=None):
    """
    Compute the ideal bandwidth allocation B_i (Mbps) for each active VN using
    an iterative weighted water-filling algorithm.

    The allocation satisfies:
        B_i = min(D_i,  g_i + w_i * theta)

    where theta >= 0 is the unique global scaling factor determined by the
    link capacity constraint:
        sum_i B_i = min(C, sum_i D_i)

    The iterative process works as follows:
      1. Distribute the residual capacity (link capacity minus the sum of
         guaranteed bandwidths of active VNs) proportionally to each VN's
         sharing weight w_i.
      2. Any VN whose tentative allocation exceeds its demand D_i is capped at
         D_i; its unused quota is released back into the residual pool.
      3. Steps 1-2 repeat until no new VN is capped, at which point the
         remaining residual is distributed proportionally among unsatisfied VNs.

    Args:
        active_vn_ids : list[int]   VN ids active in the current phase.
        demands       : dict[int, float]  Effective demand per VN (Mbps).
                        If None or a VN is absent, demand defaults to
                        TOTAL_BW_C (greedy behaviour).

    Returns:
        dict {vnid: float}  Ideal allocation in Mbps (full precision).
    """
    if not active_vn_ids:
        return {}

    if demands is None:
        demands = {}

    eff_demand = {
        vnid: min(demands.get(vnid, TOTAL_BW_C), TOTAL_BW_C)
        for vnid in active_vn_ids
    }

    active = set(active_vn_ids)
    fixed  = {}

    while True:
        non_fixed = active - set(fixed.keys())
        if not non_fixed:
            break

        # Residual capacity = link capacity minus already-fixed allocations
        # minus the guaranteed bandwidth of each non-fixed VN (capped at its
        # demand so that VNs with demand below g_i do not inflate the residual).
        residual = TOTAL_BW_C
        residual -= sum(fixed.values())
        residual -= sum(min(eff_demand[i], VNS[i]['gbw']) for i in non_fixed)
        residual  = max(residual, 0.0)

        total_w = sum(VNS[i]['weight'] for i in non_fixed)

        newly_fixed = {}
        for i in non_fixed:
            base      = min(eff_demand[i], VNS[i]['gbw'])
            share     = (residual * VNS[i]['weight'] / total_w) if total_w > 0 else 0.0
            tentative = base + share
            if tentative >= eff_demand[i]:
                newly_fixed[i] = eff_demand[i]

        if not newly_fixed:
            # No more VNs can be capped; distribute the remaining residual.
            for i in non_fixed:
                base     = min(eff_demand[i], VNS[i]['gbw'])
                share    = (residual * VNS[i]['weight'] / total_w) if total_w > 0 else 0.0
                fixed[i] = base + share
            break

        fixed.update(newly_fixed)

    return fixed


# ═══════════════════════════════════════════════════════════════════════════════
#  CSV loading
# ═══════════════════════════════════════════════════════════════════════════════

def _load_csv_results(phases):
    """
    Load per-VN analysis-window throughput samples from
    RESULT_DIR/vn{id}_throughput.csv for all active VNs.

    CSV columns: phase_name, proto, t_start_sec, throughput_mbps

    Samples are matched to phases by the phase_name field and stored as a list
    of throughput values per (phase index, VN id).  Idle phases are skipped.

    Args:
        phases : List of phase dicts as returned by _derive_phases().

    Returns:
        dict {ph_idx: {vnid: [throughput_mbps, ...]}}
    """
    name_to_idx = {
        ph['name']: idx
        for idx, ph in enumerate(phases)
        if not ph['is_idle']
    }

    results = {
        idx: {vnid: [] for vnid in ph['active_vnids']}
        for idx, ph in enumerate(phases)
        if not ph['is_idle']
    }

    for vnid in VNS:
        path = os.path.join(RESULT_DIR, 'vn{}_throughput.csv'.format(vnid))
        if not os.path.exists(path):
            print('[WARN] CSV not found, skipping: {}'.format(path))
            continue

        with open(path, 'r') as fp:
            reader = csv.DictReader(fp)
            for row in reader:
                ph_name = row['phase_name']
                mbps    = float(row['throughput_mbps'])
                if ph_name not in name_to_idx:
                    continue
                ph_idx = name_to_idx[ph_name]
                if vnid in results[ph_idx]:
                    results[ph_idx][vnid].append(mbps)

    return results


# ═══════════════════════════════════════════════════════════════════════════════
#  Table row construction
# ═══════════════════════════════════════════════════════════════════════════════

def _build_config_rows(phases):
    """
    Build data rows for the VN configuration table.

    Each row describes one VN within one phase:
        [phase_label, period, vn_label, gbw, weight, proto, injection, ideal]

    injection : For UDP sessions, the configured udp_bw rate in Mbps.
                For TCP sessions, the string 'Greedy'.
    ideal     : Ideal bandwidth allocation computed by the iterative weighted
                water-filling algorithm, rounded to 3 decimal places.

    The phase_label and period columns are filled only on the first row of each
    phase and left blank on subsequent rows to produce a visually merged cell
    effect in the table.

    Args:
        phases : List of phase dicts as returned by _derive_phases().

    Returns:
        list of row lists suitable for tabulate().
    """
    rows        = []
    phase_count = 0

    for ph in phases:
        if ph['is_idle']:
            continue
        phase_count += 1
        phase_label = 'Phase {}'.format(phase_count)
        period      = '{:.0f}-{:.0f}s'.format(ph['start'], ph['end'])

        demands   = _get_demands(ph['flow_map'])
        ideal_map = _compute_ideal(ph['active_vnids'], demands)

        for i, vnid in enumerate(ph['active_vnids']):
            vn   = VNS[vnid]
            sess = ph['flow_map'][vnid]

            injection = (
                _parse_bw_mbps(sess['udp_bw']) if sess['proto'] == 'UDP' else 'Greedy'
            )
            ideal = round(ideal_map[vnid], 3)

            rows.append([
                phase_label if i == 0 else '',
                period      if i == 0 else '',
                'VN{}'.format(vnid),
                vn['gbw'],
                vn['weight'],
                sess['proto'],
                injection,
                ideal,
            ])

    return rows


def _build_error_rows(phases, csv_results):
    """
    Build data rows for the error analysis table.

    For each active VN in each phase, the following metrics are computed from
    the analysis-window throughput samples collected by run_test.py:

        Mean              : Arithmetic mean of per-second throughput samples.
        Standard deviation: Population standard deviation (pstdev) over the
                            full analysis window.  The population form is used
                            because the analysis window represents the complete
                            measurement population of interest, not a sample
                            drawn from a larger distribution.
        Absolute error    : |Mean - Ideal|
        Relative error    : Absolute error / Ideal * 100 (%)

    All intermediate calculations use full floating-point precision; rounding
    to 3 decimal places is applied only at the final output step.  When the
    rounded absolute error equals 0.000, the relative error is also displayed
    as 0.0 for display consistency.

    Each row: [phase_label, vn_label, mean, std, abs_err, rel_err_pct]

    Args:
        phases      : List of phase dicts as returned by _derive_phases().
        csv_results : dict {ph_idx: {vnid: [throughput_mbps, ...]}} from
                      _load_csv_results().

    Returns:
        list of row lists suitable for tabulate().
    """
    rows        = []
    phase_count = 0

    for ph_idx, ph in enumerate(phases):
        if ph['is_idle']:
            continue
        phase_count += 1
        phase_label = 'Phase {}'.format(phase_count)

        demands   = _get_demands(ph['flow_map'])
        ideal_map = _compute_ideal(ph['active_vnids'], demands)

        for i, vnid in enumerate(ph['active_vnids']):
            mbps_list = csv_results.get(ph_idx, {}).get(vnid, [])

            if mbps_list:
                mean_raw = statistics.mean(mbps_list)
                # Population standard deviation: the analysis window contains
                # the full set of measurement samples, so Bessel's correction
                # is not applied.
                std_raw  = statistics.pstdev(mbps_list)
            else:
                mean_raw = std_raw = 0.0
                print('[WARN] Phase {} VN {} has no data; mean set to 0'.format(
                    phase_count, vnid))

            ideal_raw = ideal_map[vnid]

            # Compute error metrics at full precision before rounding.
            abs_err_raw = abs(mean_raw - ideal_raw)
            rel_err_raw = abs_err_raw / ideal_raw * 100 if ideal_raw else 0.0

            # Round only at the output step; each quantity is rounded
            # independently to avoid cascading rounding errors.
            mean        = round(mean_raw,    3)
            std         = round(std_raw,     3)
            abs_err     = round(abs_err_raw, 3)
            rel_err_pct = round(rel_err_raw, 3)

            # Display consistency: when the rounded absolute error is 0.000,
            # show relative error as 0.0 rather than a spurious non-zero value
            # caused by pre-rounding floating-point residuals.
            if abs_err == 0.0:
                rel_err_pct = 0.0

            rows.append([
                phase_label if i == 0 else '',
                'VN{}'.format(vnid),
                mean,
                std,
                abs_err,
                rel_err_pct,
            ])

    return rows


# ═══════════════════════════════════════════════════════════════════════════════
#  Output helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _print_table(title, headers, rows):
    """Print a human-readable table to the terminal using tabulate."""
    print('\n' + '=' * 64)
    print('  ' + title)
    print('=' * 64)
    print(tabulate(rows, headers=headers, tablefmt='grid'))


def _write_md(filename, title, headers, rows):
    """
    Write a table to RESULT_DIR/filename in GitHub Flavored Markdown format.
    The file begins with a bold title line followed by a blank line and the
    table.

    Args:
        filename : Output filename (e.g. 'table_error.md').
        title    : Bold heading written at the top of the file.
        headers  : Column header list for tabulate().
        rows     : Data row list for tabulate().
    """
    os.makedirs(RESULT_DIR, exist_ok=True)
    path = os.path.join(RESULT_DIR, filename)
    with open(path, 'w') as fp:
        fp.write('**{}**\n\n'.format(title))
        fp.write(tabulate(rows, headers=headers, tablefmt='github'))
        fp.write('\n')
    print('[analyze] wrote {}'.format(path))


# ═══════════════════════════════════════════════════════════════════════════════
#  Main function
# ═══════════════════════════════════════════════════════════════════════════════

def run_analysis():
    """
    Load CSV results, build configuration and error tables, print them to the
    terminal, and write them to Markdown files in RESULT_DIR.

    When only one non-idle phase is present, the Phase column is omitted from
    both tables to avoid a column that is either entirely blank or contains a
    single repeated value.
    """

    phases      = _derive_phases()
    csv_results = _load_csv_results(phases)
    config_rows = _build_config_rows(phases)
    error_rows  = _build_error_rows(phases, csv_results)

    config_headers = ['Phase', 'Period', 'VN',
                      'Guaranteed Bandwidth (Mbps)', 'Weight',
                      'Protocol', 'Injection (Mbps)', 'Ideal (Mbps)']
    error_headers  = ['Phase', 'VN', 'Mean (Mbps)', 'Std Dev (Mbps)',
                      'Absolute Error (Mbps)', 'Relative Error (%)']

    # Drop the Phase column when there is only one non-idle phase, as it would
    # add no information and clutter the table.
    non_idle_count = sum(1 for ph in phases if not ph['is_idle'])
    if non_idle_count == 1:
        config_headers = config_headers[1:]
        config_rows    = [r[1:] for r in config_rows]
        error_headers  = error_headers[1:]
        error_rows     = [r[1:] for r in error_rows]

    _print_table('VN Configuration', config_headers, config_rows)
    _print_table('Error Analysis',   error_headers,  error_rows)

    _write_md('table_config.md', 'VN Configuration', config_headers, config_rows)
    _write_md('table_error.md',  'Error Analysis',   error_headers,  error_rows)

    # Combined Markdown file containing both tables.
    note_path = os.path.join(RESULT_DIR, 'table_note.md')
    with open(note_path, 'w') as fp:
        fp.write('**VN Configuration**\n\n')
        fp.write(tabulate(config_rows, headers=config_headers, tablefmt='github'))
        fp.write('\n\n')
        fp.write('**Error Analysis**\n\n')
        fp.write(tabulate(error_rows, headers=error_headers, tablefmt='github'))
        fp.write('\n')
    print('[analyze] wrote {}'.format(note_path))

    print('[analyze] tables written to {}/'.format(RESULT_DIR))


# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == '__main__':
    run_analysis()