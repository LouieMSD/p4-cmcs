# p4-cmcs

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![P4](https://img.shields.io/badge/language-P4%E2%82%81%E2%82%86-orange)](https://p4.org/)
[![Platform](https://img.shields.io/badge/platform-BMv2%20simple__switch__grpc-lightgrey)](https://github.com/p4lang/behavioral-model)

**Color Marker and Color-Aware Scheduler (CMCS)** — A P4-based traffic management
mechanism for bandwidth guarantee, weighted fair sharing of residual capacity, and
in-order packet delivery in software-defined network virtualization.

Implemented in **P4₁₆** on the **BMv2 `simple_switch_grpc`** software switch,
with a modified Traffic Manager (`QueueingLogicVN`) and a Mininet-based
experiment environment.

---

## Overview

Modern cloud and enterprise networks rely on **network virtualization** to let
multiple Virtual Networks (VNs) share the same physical infrastructure. When VNs
share a physical link, three requirements must hold simultaneously:

| Goal | Description |
|---|---|
| **Bandwidth Guarantee** | Each VN receives at least its contracted bandwidth `g_i`, regardless of other VNs' traffic bursts |
| **Weighted Fair Sharing** | When the link has spare capacity, it is distributed among VNs in proportion to their weights `w_i` |
| **In-Order Packet Delivery** | Packets within the same VN always arrive at the receiver in their original order |

Concretely, each VN is assigned a guaranteed bandwidth `gᵢ` and a shared weight
`wᵢ`, with the sum of all guarantees not exceeding the link capacity. If a VN's
traffic demand falls below its guarantee, it receives exactly what it demands and
releases the unused portion into a shared pool. If its demand exceeds its
guarantee, it first receives its full guaranteed bandwidth, then competes for
additional bandwidth from the shared pool in proportion to its weight `wᵢ`. A VN
never receives more than its actual demand.

Meeting all three goals at once is non-trivial. Prior schemes (e.g.,
[P4-TINS](https://ieeexplore.ieee.org/document/9733931)) achieve bandwidth
guarantee and weighted fair sharing, but route conforming (green) and
non-conforming (yellow) packets into **separate priority queues**. This reorders
packets within the same VN, causing TCP's congestion control to misinterpret
reordering as loss — sometimes pushing throughput **below the guaranteed
bandwidth**.

**CMCS** eliminates this problem by keeping all packets of the same VN in a
**single FIFO queue**, guaranteeing in-order delivery while still meeting the
other two goals with less than 0.21% relative error in experiments.

---

## How CMCS Works

CMCS consists of **three functional modules** deployed per egress port. They form
a pipeline: packets are first classified by the Color Marker, then queued with
credit tracking, and finally scheduled by HRDS.

### 1. Color Marker Bank

Each VN has a dedicated **leaky-bucket Color Marker** that tests every arriving
packet against the VN's guaranteed bandwidth `g_i`:

- 🟢 **Green**: within the guaranteed rate → increments GB counter on enqueue
- 🟡 **Yellow**: exceeds the guaranteed rate → no counter update

The result (`vn_id`, `color`) is written to P4 user-defined metadata and read
by the Traffic Manager.

### 2. FIFO Queue Bank + Guaranteed Byte Credit (GB) Counter

Each VN maintains a **single FIFO queue** shared by both green and yellow packets.
This is the key difference from dual-priority-queue designs:

> **Why a single queue?** Dual-queue designs (e.g., P4-TINS) separate green and
> yellow packets, which reorders them within the same VN. A single FIFO queue
> preserves original arrival order — making CMCS safe for TCP traffic.

A per-VN **GB counter** `γᵢ` accumulates the byte length of each green packet
at enqueue time. When `γᵢ > 0`, the VN holds unused guaranteed-bandwidth credit
that HRDS will serve with priority.

### 3. Hybrid RR-DWRR Scheduler (HRDS)

When a transmission opportunity arises, HRDS runs two phases **in sequence**:

**Phase 1 — Round Robin (guaranteed bandwidth)**
- Scan VN queues in round-robin order
- Select the next VN with `γᵢ > 0`; dequeue one packet; deduct `γᵢ` by packet size
- Repeat until **no VN** has `γᵢ > 0`

**Phase 2 — Deficit Weighted Round Robin (residual capacity)**
- Each VN has a quantum `Qᵢ ∝ wᵢ` added to its deficit counter each round
- Dequeue packets from VNs whose deficit covers the head-of-queue packet size
- Deficits carry over across rounds, ensuring long-term byte ratio fairness

## Repository Structure

```
p4-cmcs/
├── my_project/                        # P4 program and experiment scripts
│   ├── color_markers.p4
│   ├── topo/topology.json
│   ├── utils/
│   │   ├── run_exercise.py
│   │   └── p4runtime_switch.py
│   └── test/
│       ├── run_test.py
│       ├── plot_results.py
│       └── analyze_results.py
└── behavioral-model-patches/          # Modified BMv2 source files
    ├── include/bm/bm_sim/queueing.h
    └── targets/
        ├── simple_switch/
        │   ├── simple_switch.h
        │   ├── simple_switch.cpp
        │   ├── main.cpp
        │   └── thrift/src/SimpleSwitch_server.cpp
        └── simple_switch_grpc/
            ├── main.cpp
            ├── switch_runner.h
            └── switch_runner.cpp
```

## Modified Files Overview

### my_project (P4 Program and Experiment Scripts)

| File | Description |
|---|---|
| `color_markers.p4` | Color marker P4 implementation: leaky bucket metering, VN classification, metadata write |
| `utils/run_exercise.py` | Reads `slice_weights` from topology config and passes it to the switch startup arguments |
| `utils/p4runtime_switch.py` | Forwards `--slice-weights w1,w2,...` to `simple_switch_grpc` |
| `topo/topology.json` | Topology configuration; contains `"slice_weights": [...]` field |
| `test/run_test.py` | Main script for multi-VN throughput experiments |
| `test/plot_results.py` | Plots per-second throughput time series |
| `test/analyze_results.py` | Analyzes error metrics (mean, standard deviation, absolute/relative error) |

### behavioral-model-patches (Modified BMv2 Source Files)

| File | Description |
|---|---|
| `include/bm/bm_sim/queueing.h` | Fully rewritten as `QueueingLogicVN`: added `pkt_size` to `QE`; redesigned `VNQueueInfo` / `LogicalQueueInfo` / `WorkerInfo`; four `push_front` overloads; bps leaky bucket in `pop_back`; two-phase `select_vn` (RR + DWRR); `set_rate` / `set_rate_for_all` changed to bps |
| `targets/simple_switch/simple_switch.h` | Constructor parameter changed to `vector<int> vn_weights`; added `nb_vns` member; `egress_buffers` type changed to `QueueingLogicVN`; defined `SSWITCH_VN_QUEUEING_SRC` / `SSWITCH_COLOR_SRC` |
| `targets/simple_switch/simple_switch.cpp` | Updated constructor; out-of-bounds clamp in `enqueue()`; capacity interface aligned to new method names |
| `targets/simple_switch/main.cpp` | Removed `--priority-queues`; constructor call updated to pass `{1}` |
| `targets/simple_switch/thrift/src/SimpleSwitch_server.cpp` | `set_egress_priority_queue_depth` forwarded to `set_egress_vn_queue_depth`; `set_egress_priority_queue_rate` stub returns 0 |
| `targets/simple_switch_grpc/main.cpp` | Removed `--priority-queues`; added `--slice-weights` argument parsing; defaults to `{1}` when not provided |
| `targets/simple_switch_grpc/switch_runner.h` | Parameter changed to `std::vector<int> vn_weights` |
| `targets/simple_switch_grpc/switch_runner.cpp` | Updated constructor parameter; forwarded to `SimpleSwitch` |

## Dependencies

- BMv2: https://github.com/p4lang/behavioral-model (commit: TODO)
- p4lang/tutorials: https://github.com/p4lang/tutorials (commit: TODO)
- p4c: TODO version

## How to Reproduce

1. Clone BMv2 and apply patches:
   ```bash
   git clone https://github.com/p4lang/behavioral-model
   cp -r behavioral-model-patches/include behavioral-model/
   cp -r behavioral-model-patches/targets behavioral-model/
   cd behavioral-model && ./install_deps.sh && ./autogen.sh && ./configure && make
   ```

2. Run the experiment and collect results:
   ```bash
   cd my_project
   make test
   ```
   `make test` will first bring up the virtual network topology and start
   the P4 switch (equivalent to `make run`), then automatically execute
   the test scripts. Experiment results will be saved to `my_project/test/result/`.

## License

This project is licensed under the **Apache License 2.0**.
See the [LICENSE](LICENSE) file for details.

The files under `behavioral-model-patches/` are derived from
[p4lang/behavioral-model](https://github.com/p4lang/behavioral-model),
which is also licensed under Apache 2.0.
Copyright 2013-present Barefoot Networks, Inc.