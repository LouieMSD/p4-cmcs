# p4-cmcs

Implementation of the Color Marker and Color-Aware Scheduler (CMCS)
for bandwidth guarantee and fair sharing in software-defined virtual networks,
based on P4₁₆ and BMv2 simple_switch_grpc.

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