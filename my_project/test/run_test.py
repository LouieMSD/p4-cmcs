"""
run_test.py  (timeline-based multi-VN throughput test)
=======================================================
Drives a multi-VN (Virtual Network) bandwidth experiment inside a Mininet
topology and produces per-second throughput CSVs for subsequent analysis and
plotting.

Design overview
---------------
* VN activity is declared as a list of ``sessions`` (start/end offsets, protocol,
  and target rate). Experiment phases are derived automatically from the union of
  all session boundaries — no need to declare phases manually.
* A single tshark instance runs on each receiver host for the full experiment
  duration. Multiple ``-z io,stat`` filters (one per VN source IP) are passed in
  a single invocation so that all VNs on the same receiver are captured
  atomically.
* iperf3 servers run for the entire experiment; clients are launched and stopped
  according to each session's start/end times.
* All background processes are managed via ``host.cmd('... &')``; tshark is
  stopped with SIGINT (which triggers the io,stat summary flush to disk).
* tshark output is written to /tmp and read back directly (Mininet nodes share
  the host /tmp namespace by default).

Usage
-----
    from run_test import run_experiment
    run_experiment(net)          # net: a Mininet object
"""

import os
import re
import sys
import time

AUTO_PLOT     = True   # Automatically generate throughput plots after the test
AUTO_ANALYZE  = True   # Automatically run error-analysis script after the test


# ═══════════════════════════════════════════════════════════════════════════════
#  ①  VN endpoint configuration, guaranteed bandwidth, sharing weights,
#     and active sessions
#
#  Key   = VN id (integer, 1-indexed)
#
#  Each entry must contain:
#    sender    str    Sending host name (Mininet node name)
#    receiver  str    Receiving host name
#    port      int    iperf3 listen port (must be globally unique)
#    gbw       float  Guaranteed bandwidth g_i (Mbps) — used for ideal-value
#                     calculation only; the actual enforcement is done by the
#                     leaky-bucket color marker in the P4 ingress pipeline.
#    weight    float  Sharing weight w_i — governs how residual link capacity
#                     is distributed among VNs that exceed their guaranteed
#                     bandwidth, via the Hybrid RR-DWRR Scheduler.
#    sessions  list   One or more active time windows for this VN:
#      start    float  Seconds after client t=0 when this session begins
#      end      float  Seconds after client t=0 when this session ends
#      proto    str    'UDP' or 'TCP'
#      udp_bw   str    Target L2 send rate for UDP (required when proto='UDP'),
#                      e.g. '10M'. Converted to iperf3 payload rate internally
#                      to account for UDP/IP/Ethernet header overhead.
#      udp_plen int    (optional) UDP payload size in bytes; overrides the
#                      global UDP_PAYLOAD_SIZE for this session only.
#      tcp_mss  int    (optional) TCP MSS in bytes, passed as iperf3 -M;
#                      overrides the global TCP_MSS_SIZE for this session only.
#
#  Sessions within the same VN must not overlap in time (validated at startup).
# ═══════════════════════════════════════════════════════════════════════════════

VNS = {
    # Experiment 1-1: VN 1 below guaranteed bandwidth; VN 2 and VN 3
    # both exceed their ideal allocations.  VN 1 releases unused guaranteed
    # bandwidth into the residual pool, which VN 2 and VN 3 share at 3:1.
    1: {
        'sender': 'h1', 'receiver': 'h4', 'port': 5201,
        'gbw': 3.0, 'weight': 4.0,
        'sessions': [
            {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '1M'},
        ],
    },
    2: {
        'sender': 'h2', 'receiver': 'h4', 'port': 5202,
        'gbw': 2.0, 'weight': 3.0,
        'sessions': [
            {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '8M'},
        ],
    },
    3: {
        'sender': 'h3', 'receiver': 'h4', 'port': 5203,
        'gbw': 1.0, 'weight': 1.0,
        'sessions': [
            {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '4M'},
        ],
    },

    # # Experiment 1-2: VN 1 below guaranteed bandwidth; VN 2 demand exactly
    # # meets its ideal allocation (demand-saturation case); VN 3 exceeds ideal.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 3.0, 'weight': 4.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '1M'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 3.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '4M'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '6M'},
    #     ],
    # },

    # # Experiment 1-3: All three VNs exceed both their guaranteed bandwidth and
    # # their ideal allocations — pure weighted competition for residual capacity.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 3.0, 'weight': 4.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '6M'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 3.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '5M'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '3M'},
    #     ],
    # },

    # # Experiment 1-4: All VNs exceed guaranteed bandwidth; VN 2 demand exactly
    # # meets its ideal allocation — mixed weighted competition with one saturated
    # # VN releasing its unused quota back to the residual pool.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 3.0, 'weight': 4.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '6M'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 3.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '3M'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '3M'},
    #     ],
    # },

    # # Experiment 2-1: VN idle/re-activation fairness test.
    # # VN 1 is active in phases 1 and 3 but idle in phase 2; VN 3 joins only
    # # in phases 3 and 4.  Verifies that the active-set manager clears the
    # # weighted_deficit of idle VNs so they cannot monopolize the link when
    # # they re-activate.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 3.0, 'weight': 4.0,
    #     'sessions': [
    #         {'start': 0,  'end': 30,  'proto': 'TCP'},
    #         {'start': 60, 'end': 90,  'proto': 'TCP'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 3.0,
    #     'sessions': [
    #         {'start': 0, 'end': 120, 'proto': 'TCP'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 60, 'end': 120, 'proto': 'TCP'},
    #     ],
    # },

    # # Experiment 3-1: In-order delivery vs P4-TINS comparison, case 1.
    # # VN 1 carries a greedy TCP stream; VN 2 and VN 3 inject UDP bursts.
    # # Verifies that CMCS's single FIFO queue per VN prevents TCP congestion
    # # collapse caused by out-of-order delivery, unlike P4-TINS's dual-queue
    # # design.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 4.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'TCP'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '5M'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '4M'},
    #     ],
    # },

    # # Experiment 3-2: In-order delivery vs P4-TINS comparison, case 2.
    # # VN 1 and VN 2 both carry greedy TCP streams; VN 3 injects UDP bursts.
    # # When multiple TCP streams are affected simultaneously, the out-of-order
    # # penalty compounds: each stream's congestion control independently shrinks
    # # its window, freeing disproportionate capacity for VN 3's UDP traffic.
    # 1: {
    #     'sender': 'h1', 'receiver': 'h4', 'port': 5201,
    #     'gbw': 4.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'TCP'},
    #     ],
    # },
    # 2: {
    #     'sender': 'h2', 'receiver': 'h4', 'port': 5202,
    #     'gbw': 2.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'TCP'},
    #     ],
    # },
    # 3: {
    #     'sender': 'h3', 'receiver': 'h4', 'port': 5203,
    #     'gbw': 1.0, 'weight': 1.0,
    #     'sessions': [
    #         {'start': 0, 'end': 60, 'proto': 'UDP', 'udp_bw': '6M'},
    #     ],
    # },
}

# ═══════════════════════════════════════════════════════════════════════════════
#  ②  Global UDP default parameters
#     These values apply to every session that does not specify its own
#     udp_plen / tcp_mss field.
# ═══════════════════════════════════════════════════════════════════════════════

# UDP_PAYLOAD_SIZE controls the iperf3 -l (payload length in bytes) argument.
#
# Setting a specific value (e.g. 1472) aligns the L2 frame size of UDP traffic
# with that of TCP:  1472 + 8 (UDP) + 20 (IP) = 1500 (no fragmentation),
# plus 14 (Ethernet) = 1514 bytes, identical to a full-MSS TCP frame.
# This makes L2-layer throughput comparisons between UDP and TCP fair.
#
# Setting None leaves the choice to iperf3, which probes the path MTU and
# falls back to 1460 bytes when probing fails.  The result is close to 1472
# in practice, but no longer a hard guarantee.
#
# To restore strict UDP/TCP frame-size alignment, set this to 1472 or use
# per-session udp_plen=1472 (per-session values take precedence).
UDP_PAYLOAD_SIZE = None   # iperf3 -l (bytes); None = let iperf3 decide

# TCP_MSS_SIZE controls the iperf3 -M (--set-mss) argument.
# Note: iperf3 -l for TCP only affects the userspace write buffer size, not the
# on-wire TCP segment size.  -M is the correct knob for controlling actual
# segment size.  With Ethernet MTU=1500, the maximum MSS is
# 1500 - 20 (IP) - 20 (TCP) = 1460 bytes.
# Setting None lets the OS negotiate the default MSS (typically 1460).
TCP_MSS_SIZE = None   # iperf3 -M (bytes); None = system default


# ═══════════════════════════════════════════════════════════════════════════════
#  ③  Timing and analysis parameters
#
#  Full timeline (tshark internal clock, t=0 = tshark launch):
#
#    t = 0                                  tshark + iperf3 servers start
#    t = IPERF3_START_OFFSET                first iperf3 client starts (client t=0)
#    t = IPERF3_START_OFFSET + sess.start   each session client starts
#    t = IPERF3_START_OFFSET + sess.end     each session client stops
#
#  Per-phase analysis window (tshark clock):
#    t_min = IPERF3_START_OFFSET + phase_start + TRAFFIC_WARMUP_SKIP
#    t_max = IPERF3_START_OFFSET + phase_end   - TRAFFIC_COOLDOWN_SKIP
#
#  The warmup and cooldown margins exclude transient effects such as TCP slow
#  start at the beginning of a phase and throughput drain at the end.
# ═══════════════════════════════════════════════════════════════════════════════

IPERF3_START_OFFSET   = 2   # Seconds between tshark launch and the first client
                             # start. Must be large enough for tshark to begin
                             # capturing before any traffic arrives.
TRAFFIC_WARMUP_SKIP   = 2   # Seconds trimmed from the start of each phase
                             # analysis window (excludes TCP slow-start ramp-up)
TRAFFIC_COOLDOWN_SKIP = 2   # Seconds trimmed from the end of each phase
                             # analysis window (excludes tail-end drain jitter)
TSHARK_SETTLE         = 4   # Extra seconds to wait after all traffic ends before
                             # sending SIGINT to tshark, ensuring all packets
                             # have been flushed to the io,stat summary.
MIN_ANALYSIS_WINDOW   = 5   # Minimum acceptable analysis window in seconds.
                             # Phases shorter than this threshold cause an error
                             # at startup rather than silently producing
                             # unreliable statistics.


# ═══════════════════════════════════════════════════════════════════════════════
#  ④  Ideal bandwidth calculation parameters
#
#  The ideal bandwidth B_i for each VN in each phase is computed by the
#  iterative weighted water-filling process derived from the bandwidth
#  allocation target:
#
#      B_i = min(D_i,  g_i + w_i * theta)
#
#  where theta >= 0 is the unique global scaling factor that satisfies
#
#      sum_i min(D_i, g_i + w_i * theta) = min(C, sum_i D_i).
#
#  For UDP sessions, D_i is the configured udp_bw rate.
#  For TCP greedy sessions, D_i is set to TOTAL_BW_C (full link capacity).
# ═══════════════════════════════════════════════════════════════════════════════

TOTAL_BW_C = 10.0   # Total bottleneck link capacity (Mbps)


# ═══════════════════════════════════════════════════════════════════════════════
#  ⑤  Output directory
# ═══════════════════════════════════════════════════════════════════════════════

RESULT_DIR = 'test/result'


# ───────────────────────────────────────────────────────────────────────────────
#  Startup validation
# ───────────────────────────────────────────────────────────────────────────────

def _validate():
    """
    Validate the VNS configuration before starting the experiment.
    Calls sys.exit on any violation so the error is reported before any
    Mininet processes are launched.

    Checks performed:
      1. Sessions within the same VN do not overlap in time.
      2. Every UDP session provides a udp_bw field.
      3. tcp_mss, if specified, is a positive integer.

    (Analysis-window length >= MIN_ANALYSIS_WINDOW is checked separately
     in _validate_phases, after phases have been derived.)
    """
    for vnid, vn in VNS.items():
        sessions = sorted(vn.get('sessions', []), key=lambda s: s['start'])

        # Check 1: no overlapping sessions within the same VN
        for i in range(len(sessions) - 1):
            if sessions[i]['end'] > sessions[i + 1]['start']:
                sys.exit(
                    '[ERROR] VN {vnid}: session [{s1},{e1}) overlaps with'
                    ' [{s2},{e2})'.format(
                        vnid=vnid,
                        s1=sessions[i]['start'],    e1=sessions[i]['end'],
                        s2=sessions[i+1]['start'],  e2=sessions[i+1]['end']))

        # Check 2: UDP sessions must declare a target rate
        for sess in sessions:
            if sess['proto'] == 'UDP' and 'udp_bw' not in sess:
                sys.exit(
                    '[ERROR] VN {vnid}: UDP session (start={s})'
                    ' is missing the udp_bw field'.format(
                        vnid=vnid, s=sess['start']))

        # Check 3: tcp_mss, if given, must be a positive integer
        for sess in sessions:
            if sess['proto'] == 'TCP' and 'tcp_mss' in sess:
                if not isinstance(sess['tcp_mss'], int) or sess['tcp_mss'] <= 0:
                    sys.exit(
                        '[ERROR] VN {vnid}: tcp_mss must be a positive'
                        ' integer'.format(vnid=vnid))


# ───────────────────────────────────────────────────────────────────────────────
#  Automatic phase derivation
# ───────────────────────────────────────────────────────────────────────────────

def _derive_phases():
    """
    Automatically derive experiment phases from the union of all session
    boundaries declared in VNS.

    Algorithm:
      1. Collect all session start and end times, deduplicate, and sort them
         to obtain a list of time boundaries.
      2. Each pair of adjacent boundaries defines one phase.
      3. A phase's active VN set is determined by testing whether the midpoint
         of that interval falls inside any session of each VN.

    Returns a list of dicts, each containing:
      name         str        Human-readable label, e.g. 'Phase_0s-30s'
      start        float      Phase start time (seconds, relative to client t=0)
      end          float      Phase end time
      active_vnids list[int]  VN ids active during this phase (empty = idle gap)
      flow_map     dict       {vnid: session_dict} for active VNs
      is_idle      bool       True when no VN is active
    """
    boundaries = set()
    for vn in VNS.values():
        for sess in vn['sessions']:
            boundaries.add(float(sess['start']))
            boundaries.add(float(sess['end']))
    boundaries = sorted(boundaries)

    phases = []
    for i in range(len(boundaries) - 1):
        t_s = boundaries[i]
        t_e = boundaries[i + 1]
        mid = (t_s + t_e) / 2.0

        active_vnids = []
        flow_map     = {}
        for vnid, vn in sorted(VNS.items()):   # sorted for stable ordering
            for sess in vn['sessions']:
                if sess['start'] <= mid < sess['end']:
                    active_vnids.append(vnid)
                    flow_map[vnid] = sess
                    break

        phases.append({
            'name':         'Phase_{s}s-{e}s'.format(s=int(t_s), e=int(t_e)),
            'start':        t_s,
            'end':          t_e,
            'active_vnids': active_vnids,
            'flow_map':     flow_map,
            'is_idle':      len(active_vnids) == 0,
        })

    return phases


def _validate_phases(phases):
    """
    Verify that every non-idle phase has an analysis window of at least
    MIN_ANALYSIS_WINDOW seconds after trimming the warmup and cooldown margins.
    Calls sys.exit with a descriptive message when this constraint is violated,
    directing the user to either widen the phase or reduce the skip margins.
    """
    for ph in phases:
        if ph['is_idle']:
            continue
        window = (ph['end'] - ph['start']) - TRAFFIC_WARMUP_SKIP - TRAFFIC_COOLDOWN_SKIP
        if window < MIN_ANALYSIS_WINDOW:
            sys.exit(
                '[ERROR] Phase {name} analysis window is only {w}s'
                ' (minimum {m}s). Reduce TRAFFIC_WARMUP_SKIP /'
                ' TRAFFIC_COOLDOWN_SKIP or increase the phase'
                ' duration.'.format(
                    name=ph['name'], w=window, m=MIN_ANALYSIS_WINDOW))


# ───────────────────────────────────────────────────────────────────────────────
#  Ideal bandwidth calculation helpers
# ───────────────────────────────────────────────────────────────────────────────

def _parse_bw_mbps(bw_str):
    """
    Parse an iperf3-style bandwidth string into a float (Mbps).
    Supported suffixes: G (gigabits), M (megabits), K (kilobits).
    A bare number is assumed to be in bits/s.

    Examples:
        '10M'  -> 10.0
        '500K' -> 0.5
        '1G'   -> 1000.0
    """
    s = bw_str.strip().upper()
    if s.endswith('G'):
        return float(s[:-1]) * 1000.0
    elif s.endswith('M'):
        return float(s[:-1])
    elif s.endswith('K'):
        return float(s[:-1]) / 1000.0
    else:
        return float(s) / 1e6


# ───────────────────────────────────────────────────────────────────────────────
#  L2 bandwidth → iperf3 payload bandwidth conversion
# ───────────────────────────────────────────────────────────────────────────────

UDP_OVERHEAD     = 42     # 14 (Ethernet) + 20 (IP) + 8 (UDP), in bytes
UDP_DEFAULT_PLEN = 1460   # iperf3 fallback payload size when MTU probing fails

def _l2_to_payload_bw(l2_mbps, plen):
    """
    Convert a target L2 bandwidth (Mbps) to the iperf3 -b payload-layer
    bandwidth that produces that L2 rate on the wire.

    The bottleneck link rate limit is enforced at the L2 layer (Ethernet frame
    bytes including header), so UDP injection rates are also expressed as L2
    Mbps.  iperf3, however, reports and accepts rates at the application-payload
    layer.  This function computes the correct -b argument:

        B_payload = B_L2 * P / (P + UDP_OVERHEAD)

    where P is the UDP payload size in bytes.

    Args:
        l2_mbps : Target L2 bandwidth (Mbps), as parsed from the udp_bw field.
        plen    : UDP payload size in bytes; falls back to UDP_DEFAULT_PLEN
                  when None.

    Returns:
        float  The value to pass to iperf3 -b (Mbps).
    """
    p = plen if plen else UDP_DEFAULT_PLEN
    return l2_mbps * p / (p + UDP_OVERHEAD)


# ───────────────────────────────────────────────────────────────────────────────
#  tshark output parsing
# ───────────────────────────────────────────────────────────────────────────────

def _parse_tshark_io_stat(raw, t_min, t_max):
    """
    Parse tshark ``-z io,stat,1`` output and return per-second throughput
    samples that fall entirely within the analysis window [t_min, t_max].

    tshark data-line format:
        |  1.000 <>  2.000  |   850  |  1258000  |
           t_start   t_end    frames    bytes

    Throughput conversion (L2, including Ethernet header):
        Mbps = bytes * 8 / interval_length(s) / 1e6

    Only intervals where t_start >= t_min AND t_end <= t_max are included,
    so the first and last potentially partial seconds of each phase are
    excluded from the analysis window.

    Args:
        raw   : Raw tshark output string for a single VN filter block.
        t_min : Lower bound of the analysis window (tshark internal clock, s).
        t_max : Upper bound of the analysis window (tshark internal clock, s).

    Returns:
        list of (t_start_tshark_sec, throughput_mbps) tuples.
    """
    pat = re.compile(
        r'\|\s*([\d.]+)\s*<>\s*([\d.]+)\s*\|'   # t_start <> t_end
        r'\s*\d+\s*\|'                            # frames (ignored)
        r'\s*(\d+)\s*\|'                          # bytes
    )
    rows = []
    for line in raw.splitlines():
        m = pat.search(line)
        if not m:
            continue
        t0   = float(m.group(1))
        t1   = float(m.group(2))
        byte = int(m.group(3))
        if t1 <= t0 or t0 < t_min or t1 > t_max:
            continue
        rows.append((t0, byte * 8 / (t1 - t0) / 1e6))
    return rows


def _parse_tshark_io_stat_full(raw):
    """
    Parse tshark ``-z io,stat,1`` output without any analysis-window filter.
    Used exclusively for plotting the full experiment timeline.

    The tshark internal clock is shifted so that t=0 corresponds to client t=0
    (i.e., the moment the first iperf3 client was launched).  Intervals that
    predate the first client launch (negative shifted time) are discarded to
    avoid pre-traffic noise in the plots.

    Args:
        raw : Raw tshark output string for a single VN filter block.

    Returns:
        list of (t_start_client_sec, throughput_mbps) tuples, sorted by time.
    """
    pat = re.compile(
        r'\|\s*([\d.]+)\s*<>\s*([\d.]+)\s*\|'
        r'\s*\d+\s*\|'
        r'\s*(\d+)\s*\|'
    )
    rows = []
    for line in raw.splitlines():
        m = pat.search(line)
        if not m:
            continue
        t0   = float(m.group(1))
        t1   = float(m.group(2))
        byte = int(m.group(3))
        if t1 <= t0:
            continue
        t_global = t0 - IPERF3_START_OFFSET
        if t_global < 0:   # discard pre-traffic intervals
            continue
        rows.append((t_global, byte * 8 / (t1 - t0) / 1e6))
    return rows


def _write_full_csv(full_by_vnid):
    """
    Write the complete per-second throughput time series for each VN to
    RESULT_DIR/vn{id}_full.csv, covering the entire experiment duration
    (not limited to any analysis window).

    Zero-filled intervals (inserted by the caller for seconds where tshark
    produced no output row) are included so that the plot shows a flat zero
    line during idle periods rather than a gap.

    CSV columns:
        t_start_sec        Left edge of the 1-second interval (client t=0 origin)
        throughput_mbps    Average L2 throughput over the interval
    """
    for vnid in VNS:
        path = os.path.join(
            RESULT_DIR, 'vn{vnid}_full.csv'.format(vnid=vnid))
        rows = full_by_vnid.get(vnid, [])
        with open(path, 'w') as fp:
            fp.write('t_start_sec,throughput_mbps\n')
            for t_sec, mbps in rows:
                fp.write('{t:.1f},{mbps:.4f}\n'.format(t=t_sec, mbps=mbps))
        print('[CSV] wrote {path}  ({n} intervals, full timeline)'.format(
            path=path, n=len(rows)))


def _parse_tshark_multi_filter(full_raw, filter_order):
    """
    Split tshark output containing multiple ``-z io,stat,1,"<filter>"`` blocks
    into per-VN raw strings.

    tshark marks the start of each statistics block with a header line of the
    form:
        | Col 1: <filter_str>  |

    This function scans the output line by line, switching the current block
    target each time such a header line is encountered, and accumulates all
    subsequent lines into the corresponding VN's buffer.

    Args:
        full_raw     : Complete tshark stdout as a single string.
        filter_order : List of (vnid, filter_str) pairs in the same order as
                       the -z arguments passed to tshark.  filter_str must
                       match the tshark command line exactly (e.g.
                       'ip.src == 10.0.0.1').

    Returns:
        dict {vnid: raw_block_str} where raw_block_str contains the header
        line and all data lines for that filter block.
        _parse_tshark_io_stat only matches data lines, so the header line
        does not affect parsing.
    """
    flt_to_vnid  = {flt: vnid for vnid, flt in filter_order}
    blocks       = {vnid: [] for vnid, _ in filter_order}
    current_vnid = None

    for line in full_raw.splitlines(keepends=True):
        if 'Col 1:' in line:
            for flt, vnid in flt_to_vnid.items():
                if flt in line:
                    current_vnid = vnid
                    break

        if current_vnid is not None:
            blocks[current_vnid].append(line)

    return {vnid: ''.join(lines) for vnid, lines in blocks.items()}


# ───────────────────────────────────────────────────────────────────────────────
#  CSV output
# ───────────────────────────────────────────────────────────────────────────────

def _write_csv(all_results, phases):
    """
    Write per-phase throughput samples for each VN to
    RESULT_DIR/vn{id}_throughput.csv.

    Only samples that fall within the analysis window of each phase are
    written.  Phases in which a VN is not active (including idle gap phases)
    contribute no rows.

    CSV columns:
        phase_name         Human-readable phase label (e.g. 'Phase_0s-60s')
        proto              Traffic protocol for this session ('UDP' or 'TCP')
        t_start_sec        Left edge of the 1-second interval (client t=0 origin)
        throughput_mbps    Average L2 throughput over the interval
    """
    for vnid in VNS:
        path = os.path.join(
            RESULT_DIR, 'vn{vnid}_throughput.csv'.format(vnid=vnid))
        with open(path, 'w') as fp:
            fp.write('phase_name,proto,t_start_sec,throughput_mbps\n')
            for ph_idx, ph in enumerate(phases):
                if ph['is_idle'] or vnid not in ph['active_vnids']:
                    continue
                proto = ph['flow_map'][vnid]['proto']
                for t_global, mbps in all_results[ph_idx].get(vnid, []):
                    fp.write('{name},{proto},{t:.1f},{mbps:.4f}\n'.format(
                        name=ph['name'], proto=proto,
                        t=t_global, mbps=mbps))
        print('[CSV] wrote {path}'.format(path=path))


# ───────────────────────────────────────────────────────────────────────────────
#  Main entry point
# ───────────────────────────────────────────────────────────────────────────────

def run_experiment(net):
    """
    Execute the full multi-VN throughput experiment and write result CSVs.

    Called by utils/run_exercise.py after the Mininet topology and P4 switch
    have been initialized.

    Args:
        net : A Mininet net object with all hosts and switches already started.
    """

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 0: Validate configuration, derive phases, print startup summary
    # ══════════════════════════════════════════════════════════════════════════

    _validate()
    phases = _derive_phases()
    _validate_phases(phases)
    os.makedirs(RESULT_DIR, exist_ok=True)

    print('\n[INFO] Timing parameters:')
    print('  IPERF3_START_OFFSET   = {v}s  (tshark -> iperf3 launch delay)'.format(
        v=IPERF3_START_OFFSET))
    print('  TRAFFIC_WARMUP_SKIP   = {v}s  (trimmed from the start of each phase)'.format(
        v=TRAFFIC_WARMUP_SKIP))
    print('  TRAFFIC_COOLDOWN_SKIP = {v}s  (trimmed from the end of each phase)'.format(
        v=TRAFFIC_COOLDOWN_SKIP))
    print('  MIN_ANALYSIS_WINDOW   = {v}s  (minimum valid analysis window)'.format(
        v=MIN_ANALYSIS_WINDOW))

    print('\n[INFO] Derived phases ({n} total):'.format(n=len(phases)))
    for ph in phases:
        if ph['is_idle']:
            print('  {name:<22}  <- idle gap (skipped)'.format(name=ph['name']))
        else:
            win = (ph['end'] - ph['start']) - TRAFFIC_WARMUP_SKIP - TRAFFIC_COOLDOWN_SKIP
            print('  {name:<22}  VN={ids}  analysis window={win}s'.format(
                name=ph['name'], ids=ph['active_vnids'], win=win))

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 1: Connectivity check
    # ══════════════════════════════════════════════════════════════════════════

    print('\n[INFO] Running pingAll connectivity check...')
    loss = net.pingAll()
    if loss > 0:
        print('[WARN] pingAll packet loss {:.1f}% — network may be unstable!'.format(loss))
    else:
        print('[INFO] pingAll passed.')

    # ══════════════════════════════════════════════════════════════════════════
    #  Pre-processing: build receiver -> [(vnid, sender_ip)] mapping
    # ══════════════════════════════════════════════════════════════════════════

    # Maps each receiver host to the ordered list of (vnid, sender_ip) pairs
    # that will send traffic to it.  The ordering must match the -z filter
    # arguments passed to tshark so that _parse_tshark_multi_filter can
    # correctly demultiplex the output blocks.
    receiver_senders = {}
    for vnid, vn in sorted(VNS.items()):
        rcv = vn['receiver']
        if rcv not in receiver_senders:
            receiver_senders[rcv] = []
        receiver_senders[rcv].append((vnid, net.get(vn['sender']).IP()))

    # Total experiment duration measured from client t=0
    total_end = max(
        sess['end']
        for vn in VNS.values()
        for sess in vn['sessions'])

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 2: Launch iperf3 servers (run for the entire experiment duration)
    # ══════════════════════════════════════════════════════════════════════════

    print('\n[INFO] Starting iperf3 servers...')
    for vnid, vn in sorted(VNS.items()):
        net.get(vn['receiver']).cmd(
            'iperf3 -s -p {port} > /dev/null 2>&1 &'.format(port=vn['port']))
        print('  iperf3 server  {rcv}  port={port}'.format(
            rcv=vn['receiver'], port=vn['port']))
    time.sleep(0.5)

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 3: Launch tshark (one instance per receiver, multiple -z filters)
    #
    #  A single tshark process per receiver captures traffic from all VN senders
    #  simultaneously.  Per-VN traffic is separated by source IP using multiple
    #  -z io,stat,1,"ip.src == <IP>" filters in one invocation.  This avoids
    #  the timing skew that would arise from starting separate tshark instances.
    #
    #  tshark stdout is redirected to /tmp/tshark_<rcv>.out.  SIGINT (sent
    #  after all traffic has finished) triggers tshark to flush and write the
    #  final io,stat summary before exiting.
    # ══════════════════════════════════════════════════════════════════════════

    print('\n[INFO] Starting tshark...')
    tshark_files        = {}   # receiver -> output filepath
    tshark_filter_order = {}   # receiver -> [(vnid, filter_str), ...]

    for rcv, sender_list in sorted(receiver_senders.items()):
        rx_node  = net.get(rcv)
        intf     = rx_node.defaultIntf().name
        fpath    = '/tmp/tshark_{rcv}.out'.format(rcv=rcv)
        tshark_files[rcv] = fpath

        filter_args = ''
        order       = []
        for vnid, src_ip in sender_list:
            flt          = 'ip.src == {ip}'.format(ip=src_ip)
            filter_args += ' -z io,stat,1,"{flt}"'.format(flt=flt)
            order.append((vnid, flt))
        tshark_filter_order[rcv] = order

        rx_node.cmd(
            'tshark -i {intf}{filters} -q > {fpath} 2>&1 &'.format(
                intf=intf, filters=filter_args, fpath=fpath))
        print('  tshark @ {rcv}  intf={intf}  VN={vnids}  -> {fpath}'.format(
            rcv=rcv, intf=intf,
            vnids=[vnid for vnid, _ in order],
            fpath=fpath))

    time.sleep(1.0)   # Allow tshark to initialize and begin capturing

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 4: Schedule iperf3 clients according to session start/end times
    #
    #  All sessions are sorted by start time.  After sleeping for
    #  IPERF3_START_OFFSET seconds (so tshark is already running), wall-clock
    #  time is recorded as client t=0.  Each client is launched by sleeping
    #  until its session.start offset from that reference point.  The client
    #  duration (-t) is set to (session.end - session.start) so it exits
    #  automatically.
    # ══════════════════════════════════════════════════════════════════════════

    all_sessions = sorted(
        [(vnid, VNS[vnid], sess)
         for vnid, vn in VNS.items()
         for sess in vn['sessions']],
        key=lambda x: x[2]['start'])

    print('\n[INFO] Sleeping {v}s (IPERF3_START_OFFSET) before client launch...'.format(
        v=IPERF3_START_OFFSET))
    time.sleep(IPERF3_START_OFFSET)

    client_t0 = time.time()   # Wall-clock reference for client t=0
    print('[INFO] Client scheduling started (client t=0 recorded).')

    for vnid, vn, sess in all_sessions:
        elapsed = time.time() - client_t0
        wait    = sess['start'] - elapsed
        if wait > 0:
            time.sleep(wait)

        duration = sess['end'] - sess['start']
        rip      = net.get(vn['receiver']).IP()
        tx       = net.get(vn['sender'])

        if sess['proto'] == 'UDP':
            # Convert the L2 target rate to the iperf3 payload rate so that
            # the on-wire L2 throughput (including Ethernet/IP/UDP headers)
            # matches the configured udp_bw value.
            l2_mbps    = _parse_bw_mbps(sess['udp_bw'])
            plen       = sess.get('udp_plen', UDP_PAYLOAD_SIZE)
            bw_payload = _l2_to_payload_bw(l2_mbps, plen)
            bw_arg     = '{:.4f}M'.format(bw_payload)
            plen_arg   = ' -l {plen}'.format(plen=plen) if plen else ''
            cmd        = (
                'iperf3 -c {ip} -p {port} -u -b {bw}{plen_arg}'
                ' -t {dur} > /dev/null 2>&1 &'
            ).format(ip=rip, port=vn['port'],
                     bw=bw_arg, plen_arg=plen_arg, dur=int(duration))
            used_plen  = plen if plen else UDP_DEFAULT_PLEN
            size_info  = (
                'udp_plen={p}B  '
                'L2_target={l2:.3f}M -> iperf3_payload={pl:.4f}M'.format(
                    p=used_plen, l2=l2_mbps, pl=bw_payload)
            )
        else:
            mss     = sess.get('tcp_mss', TCP_MSS_SIZE)
            mss_arg = ' -M {mss}'.format(mss=mss) if mss else ''
            cmd     = (
                'iperf3 -c {ip} -p {port}{mss_arg}'
                ' -t {dur} > /dev/null 2>&1 &'
            ).format(ip=rip, port=vn['port'], mss_arg=mss_arg, dur=int(duration))
            size_info = (
                'tcp_mss={m}B'.format(m=mss) if mss
                else 'tcp_mss=system default (~1460B)'
            )

        tx.cmd(cmd)
        t_now = time.time() - client_t0
        print('  t={t:.2f}s  VN {vnid} [{proto}] started'
              '  {snd} -> {rcv}  port={port}  dur={dur}s  {size}'.format(
                  t=t_now, vnid=vnid, proto=sess['proto'],
                  snd=vn['sender'], rcv=vn['receiver'],
                  port=vn['port'], dur=int(duration),
                  size=size_info))

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 5: Wait for all traffic to finish, then allow tshark to flush
    # ══════════════════════════════════════════════════════════════════════════

    elapsed   = time.time() - client_t0
    remaining = total_end - elapsed + TSHARK_SETTLE
    if remaining > 0:
        print('\n[INFO] Waiting {r:.1f}s (remaining traffic + TSHARK_SETTLE={ts}s)...'.format(
            r=remaining, ts=TSHARK_SETTLE))
        time.sleep(remaining)

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 6: Stop tshark (SIGINT triggers io,stat summary flush to file)
    #
    #  pkill is executed in the host PID namespace so it can reach Mininet
    #  node processes, which are visible as regular host processes.
    # ══════════════════════════════════════════════════════════════════════════

    print('[INFO] Stopping tshark (pkill -INT)...')
    os.system('pkill -INT -f tshark 2>/dev/null')
    time.sleep(1.5)   # Allow tshark to complete the flush and exit cleanly

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 7: Clean up residual iperf3 processes
    # ══════════════════════════════════════════════════════════════════════════

    cleaned_tx, cleaned_rx = set(), set()
    for vnid, vn in VNS.items():
        if vn['sender'] not in cleaned_tx:
            net.get(vn['sender']).cmd("pkill -f 'iperf3 -c' 2>/dev/null; true")
            cleaned_tx.add(vn['sender'])
        if vn['receiver'] not in cleaned_rx:
            net.get(vn['receiver']).cmd("pkill -f 'iperf3 -s' 2>/dev/null; true")
            cleaned_rx.add(vn['receiver'])
    time.sleep(0.3)

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 8: Read tshark output files and demultiplex per-VN filter blocks
    #
    #  Mininet nodes share the host /tmp namespace by default, so the files
    #  written by tshark can be opened directly without any file transfer.
    # ══════════════════════════════════════════════════════════════════════════

    raw_by_vnid = {}   # {vnid: raw_block_str}
    for rcv, fpath in sorted(tshark_files.items()):
        try:
            with open(fpath, 'r') as fp:
                full_raw = fp.read()
        except IOError:
            print('[WARN] tshark output file not found: {f}'.format(f=fpath))
            full_raw = ''

        parsed = _parse_tshark_multi_filter(full_raw, tshark_filter_order[rcv])
        raw_by_vnid.update(parsed)

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 8b: Parse the full timeline (no window filter) for plotting
    #
    #  tshark does not emit rows for zero-traffic seconds, so missing integers
    #  in the time series are filled with 0.0 Mbps.  This ensures that the
    #  throughput plot shows a flat zero line during idle phases rather than
    #  leaving gaps between active segments.
    # ══════════════════════════════════════════════════════════════════════════

    full_by_vnid = {}
    for vnid in VNS:
        raw  = raw_by_vnid.get(vnid, '')
        rows = _parse_tshark_io_stat_full(raw)

        # Zero-fill missing seconds
        existing_t = {t for t, _ in rows}
        for t_sec in range(0, int(total_end) + int(TSHARK_SETTLE)):
            if float(t_sec) not in existing_t:
                rows.append((float(t_sec), 0.0))
        rows.sort(key=lambda x: x[0])

        full_by_vnid[vnid] = rows
        print('  [full] VN {vnid}  {n} 1s intervals (including zero-fill)'.format(
            vnid=vnid, n=len(rows)))

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 9: Extract per-phase analysis-window samples
    #
    #  For each non-idle phase, the tshark internal clock bounds are:
    #    t_min = IPERF3_START_OFFSET + phase.start + TRAFFIC_WARMUP_SKIP
    #    t_max = IPERF3_START_OFFSET + phase.end   - TRAFFIC_COOLDOWN_SKIP
    #
    #  Parsed timestamps are then shifted back to client t=0 for CSV output:
    #    t_global = t_tshark - IPERF3_START_OFFSET
    # ══════════════════════════════════════════════════════════════════════════

    all_results = {}   # {ph_idx: {vnid: [(t_global_sec, mbps), ...]}}
    sep = '─' * 64

    print('\n[INFO] Parsing per-phase analysis windows:')
    print(sep)

    for ph_idx, ph in enumerate(phases):
        if ph['is_idle']:
            all_results[ph_idx] = {}
            print('  {name}  (idle gap, skipped)'.format(name=ph['name']))
            continue

        t_min = IPERF3_START_OFFSET + ph['start'] + TRAFFIC_WARMUP_SKIP
        t_max = IPERF3_START_OFFSET + ph['end']   - TRAFFIC_COOLDOWN_SKIP

        print('  > {name}  '
              'tshark window t={mn:.0f}~{mx:.0f}s  '
              'VN={ids}'.format(
                  name=ph['name'], mn=t_min, mx=t_max,
                  ids=ph['active_vnids']))

        phase_results = {}
        for vnid in ph['active_vnids']:
            raw  = raw_by_vnid.get(vnid, '')
            rows = _parse_tshark_io_stat(raw, t_min, t_max)

            # Shift from tshark clock to client t=0 for CSV output
            rows_global = [
                (t0 - IPERF3_START_OFFSET, mbps)
                for t0, mbps in rows
            ]
            phase_results[vnid] = rows_global

            if rows_global:
                avg = sum(m for _, m in rows_global) / len(rows_global)
                print('    VN {vnid}  mean={avg:.3f} Mbps'
                      '  ({n} intervals)'.format(
                          vnid=vnid, avg=avg, n=len(rows_global)))
            else:
                print('    VN {vnid}  [WARN] no valid data'.format(vnid=vnid))
                print('    tshark raw output (first 800 chars):')
                print(raw[:800] if raw else '    (file is empty)')

        all_results[ph_idx] = phase_results

    print(sep)

    # ══════════════════════════════════════════════════════════════════════════
    #  Stage 10: Write CSV output files
    # ══════════════════════════════════════════════════════════════════════════

    print('\n' + '=' * 64)
    print('  >> Writing CSV files')
    print('=' * 64)
    _write_csv(all_results, phases)
    _write_full_csv(full_by_vnid)

    # Optionally generate throughput plots and run error analysis
    if AUTO_PLOT:
        from plot_results import plot_results
        plot_results()

    if AUTO_ANALYZE:
        import subprocess
        script = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'analyze_results.py')
        subprocess.call(['python3', script])