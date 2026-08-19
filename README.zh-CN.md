# p4-cmcs

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![P4](https://img.shields.io/badge/language-P4%E2%82%81%E2%82%86-orange)](https://p4.org/)
[![Platform](https://img.shields.io/badge/platform-BMv2%20simple__switch__grpc-lightgrey)](https://github.com/p4lang/behavioral-model)

[English](README.md) | [繁體中文](README.zh-TW.md) | 简体中文

**颜色标记器与颜色感知调度器（Color Marker and Color-Aware Scheduler, CMCS）** — 一套基于 P4 的流量管理机制，用于在软件定义网络虚拟化环境中实现带宽保证、剩余容量加权公平共享与数据包顺序传递。

以 **P4₁₆** 语言实现于 **BMv2 `simple_switch_grpc`** 软件交换机，搭配经改写的流量管理器（`QueueingLogicVN`）与基于 Mininet 的实验环境。

---

## 概述

现代云计算与企业网络依赖**网络虚拟化**技术，使多个虚拟网络（Virtual Network, VN）得以共享同一套物理基础设施。当多个 VN 共享同一条物理链路时，须同时满足以下三项设计目标：

| 目标 | 描述 |
|---|---|
| **带宽保证** | 每个 VN 至少获得其约定的保证带宽 `g_i`，不受其他 VN 流量突发的影响 |
| **加权公平共享** | 当链路存在剩余容量时，依各 VN 的共享权重 `w_i` 按比例分配 |
| **数据包顺序传递** | 同一 VN 内的数据包始终以原始到达顺序抵达接收端 |

具体而言，每个 VN 被分配一个保证带宽 `gᵢ` 与一个共享权重 `wᵢ`，所有 VN 的保证带宽之和不超过链路容量。若某 VN 的流量需求低于其保证带宽，则该 VN 仅获得其实际需求的带宽，并将未使用的部分释放至共享池；若需求超出保证带宽，则先获得完整的保证带宽，再依共享权重 `wᵢ` 竞争共享池中的剩余容量。任何 VN 获得的带宽均不超过其实际需求。

同时达成上述三项目标并不容易。现有方案（如 [P4-TINS](https://ieeexplore.ieee.org/document/9733931)）虽能实现带宽保证与加权公平共享，但将合规（绿色）数据包与不合规（黄色）数据包分入**不同优先级的队列**，导致同一 VN 内的数据包乱序，使 TCP 拥塞控制机制将乱序误判为数据包丢失，有时令实际吞吐量**低于保证带宽**。

**CMCS** 通过将同一 VN 的所有数据包保持在**单一 FIFO 队列**中，从根本上消除上述问题，在确保数据包顺序传递的同时，精确达成另外两项设计目标，并已通过实验加以验证。

---

## CMCS 的工作原理

CMCS 由**三个功能模块**组成，部署于每个出口端口。三个模块依序串联，形成完整的处理流程：数据包首先经颜色标记器判定合规性，再进入 FIFO 队列并追踪保证字节信用，最后由 HRDS 调度器决定出队顺序。

### 1. 颜色标记器组（Color Marker Bank）

每个 VN 配有一个专属的**漏桶式颜色标记器**，依保证带宽 `g_i` 对每个到达数据包进行合规性判定：

- 🟢 **绿色**：处于保证带宽范围之内 → 入队时累积 GB 计数器
- 🟡 **黄色**：超出保证带宽范围 → 不更新计数器

判定结果（`vn_id`、`color`）写入 P4 的 user-defined metadata，供流量管理器读取。

### 2. 先进先出队列组（FIFO Queue Bank）与保证字节信用（GB）计数器

每个 VN 维护一条**单一 FIFO 队列**，绿色与黄色数据包共用同一条队列。这是 CMCS 与双优先级队列设计的根本差异：

> **为何采用单一队列？** 双队列设计（如 P4-TINS）将绿色与黄色数据包分流至不同队列，导致同一 VN 内的数据包乱序，对 TCP 流量造成严重影响。CMCS 采用单一 FIFO 队列，严格保持数据包原始到达顺序，确保 TCP 流量的服务质量。

每个 VN 各自维护一个 **GB 计数器** `γᵢ`，在每个绿色数据包入队时以其字节长度累加。`γᵢ > 0` 表示该 VN 仍有尚未兑现的保证带宽信用，HRDS 将优先为其提供发送机会。

### 3. 混合轮询-差额加权轮询调度器（Hybrid RR-DWRR Scheduler, HRDS）

当链路出现发送机会时，HRDS **依序**执行两个调度阶段：

**第一阶段 — 轮询（Round Robin）：服务保证带宽流量**
- 以环绕方式依序扫描各 VN 队列
- 选取下一个 `γᵢ > 0` 的 VN，出队一个数据包，并以数据包大小扣减 `γᵢ`
- 持续执行，直至**所有 VN** 的 `γᵢ` 均降至非正值

**第二阶段 — 差额加权轮询（Deficit Weighted Round Robin, DWRR）：分配剩余容量**
- 每轮为各 VN 的差额计数器补充与共享权重 `wᵢ` 成正比的调度配额 `Qᵢ`
- 差额计数器足以覆盖队列首部数据包大小时，即可出队发送
- 差额跨轮延续，确保长期字节比例公平

## 仓库结构

```
p4-cmcs/
├── my_project/                        # P4 程序与实验脚本
│   ├── color_markers.p4
│   ├── topo/topology.json
│   ├── utils/
│   │   ├── run_exercise.py
│   │   └── p4runtime_switch.py
│   └── test/
│       ├── run_test.py
│       ├── plot_results.py
│       └── analyze_results.py
└── behavioral-model-patches/          # 修改后的 BMv2 源码
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

## 修改文件说明

### my_project（P4 程序与实验脚本）

| 文件 | 说明 |
|---|---|
| `color_markers.p4` | 颜色标记器的 P4 实现：漏桶计量、VN 分类、metadata 写入 |
| `utils/run_exercise.py` | 从拓扑配置文件读取 `slice_weights`，并作为交换机启动参数传入 |
| `utils/p4runtime_switch.py` | 将 `--slice-weights w1,w2,...` 转发至 `simple_switch_grpc` |
| `topo/topology.json` | 拓扑配置文件，包含 `"slice_weights": [...]` 字段 |
| `test/run_test.py` | 多 VN 吞吐量实验的主控脚本 |
| `test/plot_results.py` | 绘制逐秒吞吐量时序折线图 |
| `test/analyze_results.py` | 分析误差指标（均值、标准差、绝对误差与相对误差） |

### behavioral-model-patches（修改后的 BMv2 源码）

| 文件 | 说明 |
|---|---|
| `include/bm/bm_sim/queueing.h` | 完整改写为 `QueueingLogicVN`：在 `QE` 中新增 `pkt_size`；重新设计 `VNQueueInfo` / `LogicalQueueInfo` / `WorkerInfo`；四个 `push_front` 重载；`pop_back` 中的 bps 漏桶；两阶段 `select_vn`（RR + DWRR）；`set_rate` / `set_rate_for_all` 改为 bps 单位 |
| `targets/simple_switch/simple_switch.h` | 构造函数参数改为 `vector<int> vn_weights`；新增 `nb_vns` 成员；`egress_buffers` 类型改为 `QueueingLogicVN`；定义 `SSWITCH_VN_QUEUEING_SRC` / `SSWITCH_COLOR_SRC` |
| `targets/simple_switch/simple_switch.cpp` | 更新构造函数；`enqueue()` 中的越界截断；容量接口对齐新方法名称 |
| `targets/simple_switch/main.cpp` | 移除 `--priority-queues`；更新构造函数调用，传入 `{1}` |
| `targets/simple_switch/thrift/src/SimpleSwitch_server.cpp` | `set_egress_priority_queue_depth` 转发至 `set_egress_vn_queue_depth`；`set_egress_priority_queue_rate` 桩函数返回 0 |
| `targets/simple_switch_grpc/main.cpp` | 移除 `--priority-queues`；新增 `--slice-weights` 参数解析；未提供时默认为 `{1}` |
| `targets/simple_switch_grpc/switch_runner.h` | 参数改为 `std::vector<int> vn_weights` |
| `targets/simple_switch_grpc/switch_runner.cpp` | 更新构造函数参数；转发至 `SimpleSwitch` |

## 依赖项

- [BMv2](https://github.com/p4lang/behavioral-model)（commit：`392f801`）
- [p4lang/tutorials](https://github.com/p4lang/tutorials)（commit：`ce7d49f`）
- p4c `1.2.5.11`
- [Mininet](http://mininet.org/) `2.3.1b4`
- Python `3.12.3`
- iperf3
- tshark

## 复现方法

### 前置条件

请确保已安装完整的 P4 开发环境，包含 `p4c`、`simple_switch_grpc` 与 Mininet。环境安装方式请参考 [p4lang/tutorials](https://github.com/p4lang/tutorials) 的安装脚本，各依赖项的确切版本请见[依赖项](#依赖项)一节。

### 1. 克隆仓库

```bash
git clone https://github.com/<your-username>/p4-cmcs
cd p4-cmcs
```

### 2. 应用补丁并重新编译 BMv2

将修改后的源码复制至 BMv2 源码树，再重新编译并安装：

```bash
cp -r behavioral-model-patches/include ~/src/behavioral-model/
cp -r behavioral-model-patches/targets ~/src/behavioral-model/

cd ~/src/behavioral-model
make clean
./autogen.sh
./configure --with-pi
make -j$(nproc)
sudo make install
sudo ldconfig
```

### 3. 运行实验并收集结果

```bash
cd <path-to-p4-cmcs>/my_project
make test
```

`make test` 将先构建虚拟网络拓扑并启动 P4 交换机（相当于 `make run`），再自动执行测试脚本。实验结果将保存至 `my_project/test/result/`。

## 许可证

本项目采用 **Apache License 2.0** 许可证。详情请参阅 [LICENSE](LICENSE) 文件。

`behavioral-model-patches/` 目录下的文件衍生自 [p4lang/behavioral-model](https://github.com/p4lang/behavioral-model)，同样采用 Apache 2.0 许可证。
Copyright 2013-present Barefoot Networks, Inc.