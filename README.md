# p4-cmcs

**Color Marker and Color-Aware Scheduler (CMCS)** — A P4-based traffic management
mechanism for bandwidth guarantee, weighted fair sharing of residual capacity, and
in-order packet delivery in software-defined network virtualization.

Implemented in **P4₁₆** on the **BMv2 `simple_switch_grpc`** software switch,
with a modified Traffic Manager (`QueueingLogicVN`) and a Mininet-based
experiment environment.

---

## Overview

In network virtualization, multiple Virtual Networks (VNs) often share the same
physical link. Without proper traffic management, link capacity is distributed
arbitrarily based on arrival timing and burst intensity, offering no bandwidth
guarantees and no fair residual capacity sharing.

**CMCS** addresses this by achieving three design goals simultaneously:

| Goal | Description |
|---|---|
| **Bandwidth Guarantee** | Each VN receives at least its contracted guaranteed bandwidth `g_i` |
| **Weighted Fair Sharing** | Residual capacity is distributed among VNs proportionally to their weights `w_i` |
| **In-Order Packet Delivery** | Packets within the same VN are always delivered in their original arrival order |

Most existing schemes (e.g., P4-TINS) achieve the first two goals but sacrifice
in-order delivery by routing green (conforming) and yellow (non-conforming) packets
into separate priority queues. This causes TCP congestion control to misinterpret
reordering as loss, severely degrading throughput — sometimes below the guaranteed
bandwidth. CMCS eliminates this problem entirely.

---

## How CMCS Works

CMCS consists of three functional modules deployed per egress port:

### 1. Color Marker Bank
Each VN has a dedicated **leaky-bucket Color Marker** that classifies every
arriving packet against the VN's guaranteed bandwidth `g_i`:

- **Green**: packet is within the guaranteed bandwidth rate
- **Yellow**: packet exceeds the guaranteed bandwidth rate

The classification result (`vn_id`, `color`) is written into P4 user-defined
metadata and passed to the Traffic Manager.

### 2. FIFO Queue Bank with Guaranteed Byte Credit (GB) Counters
Each VN maintains a **single shared FIFO queue** for both green and yellow packets,
preserving original arrival order within each VN. A per-VN **GB counter** `γ_i`
accumulates the byte length of each green packet upon enqueue. Yellow packets do
not increment the counter.

> Unlike dual-priority-queue designs (e.g., P4-TINS), the single-queue design
> ensures packets are never reordered within a VN, making CMCS safe for TCP traffic.

### 3. Hybrid RR-DWRR Scheduler (HRDS)
When a transmission opportunity arises, HRDS makes a two-phase scheduling decision:

- **Phase 1 — Round Robin (RR):** Scan all VN queues in round-robin order;
  if a VN has `γ_i > 0`, dequeue one packet and deduct `γ_i` by the packet size.
  This phase prioritizes guaranteed bandwidth traffic.
- **Phase 2 — Deficit Weighted Round Robin (DWRR):** When no VN has a positive
  GB counter, distribute residual capacity proportionally to shared weights `w_i`
  using DWRR with per-VN deficit counters that carry over across rounds.

The bandwidth allocation achieved by CMCS converges to the following target:

$$
B_i = \min\!\left(D_i,\; g_i + w_i\theta\right)
$$

where `θ ≥ 0` is a global scaling factor uniquely determined by the link capacity
constraint, and `D_i` is VN `i`'s traffic demand.

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

MIT