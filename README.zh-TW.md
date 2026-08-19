# p4-cmcs

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![P4](https://img.shields.io/badge/language-P4%E2%82%81%E2%82%86-orange)](https://p4.org/)
[![Platform](https://img.shields.io/badge/platform-BMv2%20simple__switch__grpc-lightgrey)](https://github.com/p4lang/behavioral-model)

[English](README.md) | 繁體中文 | [简体中文](README.zh-CN.md)

**顏色標記器與顏色感知排程器（Color Marker and Color-Aware Scheduler, CMCS）** — 一套基於 P4 的流量管理機制，用於在軟體定義網路虛擬化環境中實現頻寬保證、剩餘容量加權公平共享與封包依序遞送。

以 **P4₁₆** 語言實作於 **BMv2 `simple_switch_grpc`** 軟體交換機，搭配經改寫的流量管理器（`QueueingLogicVN`）與基於 Mininet 的實驗環境。

---

## 概述

現代雲端與企業網路仰賴**網路虛擬化**技術，使多個虛擬網路（Virtual Network, VN）得以共享同一套實體基礎設施。當多個 VN 共享同一條實體鏈路時，須同時滿足以下三項設計目標：

| 目標 | 描述 |
|---|---|
| **頻寬保證** | 每個 VN 至少獲得其約定的保證頻寬 `g_i`，不受其他 VN 流量突發的影響 |
| **加權公平共享** | 當鏈路存在剩餘容量時，依各 VN 的共享權重 `w_i` 按比例分配 |
| **封包依序遞送** | 同一 VN 內的封包始終以原始到達順序抵達接收端 |

具體而言，每個 VN 被分配一個保證頻寬 `gᵢ` 與一個共享權重 `wᵢ`，所有 VN 的保證頻寬之和不超過鏈路容量。若某 VN 的流量需求低於其保證頻寬，則該 VN 僅獲得其實際需求的頻寬，並將未使用的部分釋放至共享池；若需求超出保證頻寬，則先獲得完整的保證頻寬，再依共享權重 `wᵢ` 競爭共享池中的剩餘容量。任何 VN 獲得的頻寬均不超過其實際需求。

同時達成上述三項目標並不容易。現有方案（如 [P4-TINS](https://ieeexplore.ieee.org/document/9733931)）雖能實現頻寬保證與加權公平共享，但將合規（綠色）封包與不合規（黃色）封包分入**不同優先權的佇列**，導致同一 VN 內的封包失序，使 TCP 壅塞控制機制將失序誤判為封包遺失，有時令實際吞吐量**低於保證頻寬**。

**CMCS** 透過將同一 VN 的所有封包保持在**單一 FIFO 佇列**中，從根本上消除上述問題，在確保封包依序遞送的同時，精確達成另外兩項設計目標，並已透過實驗加以驗證。

---

## CMCS 的運作原理

CMCS 由**三個功能模組**組成，部署於每個輸出埠。三個模組依序串接，形成完整的處理流程：封包首先經顏色標記器判定合規性，再進入 FIFO 佇列並追蹤保證位元組信用，最後由 HRDS 排程器決定出列順序。

### 1. 顏色標記器組（Color Marker Bank）

每個 VN 配有一個專屬的**漏桶式顏色標記器**，依保證頻寬 `g_i` 對每個到達封包進行合規性判定：

- 🟢 **綠色**：處於保證頻寬範圍之內 → 入列時累積 GB 計數器
- 🟡 **黃色**：超出保證頻寬範圍 → 不更新計數器

判定結果（`vn_id`、`color`）寫入 P4 的 user-defined metadata，供流量管理器讀取。

### 2. 先進先出佇列組（FIFO Queue Bank）與保證位元組信用（GB）計數器

每個 VN 維護一條**單一 FIFO 佇列**，綠色與黃色封包共用同一條佇列。這是 CMCS 與雙優先權佇列設計的根本差異：

> **為何採用單一佇列？** 雙佇列設計（如 P4-TINS）將綠色與黃色封包分流至不同佇列，導致同一 VN 內的封包失序，對 TCP 流量造成嚴重影響。CMCS 採用單一 FIFO 佇列，嚴格保持封包原始到達順序，確保 TCP 流量的服務品質。

每個 VN 各自維護一個 **GB 計數器** `γᵢ`，在每個綠色封包入列時以其位元組長度累加。`γᵢ > 0` 表示該 VN 仍有尚未兌現的保證頻寬信用，HRDS 將優先為其提供發送機會。

### 3. 混合輪詢-差額加權輪詢排程器（Hybrid RR-DWRR Scheduler, HRDS）

當鏈路出現發送機會時，HRDS **依序**執行兩個排程階段：

**第一階段 — 輪詢（Round Robin）：服務保證頻寬流量**
- 以環繞方式依序掃描各 VN 佇列
- 選取下一個 `γᵢ > 0` 的 VN，出列一個封包，並以封包大小扣減 `γᵢ`
- 持續執行，直至**所有 VN** 的 `γᵢ` 均降至非正值

**第二階段 — 差額加權輪詢（Deficit Weighted Round Robin, DWRR）：分配剩餘容量**
- 每輪為各 VN 的差額計數器補充與共享權重 `wᵢ` 成正比的排程量子 `Qᵢ`
- 差額計數器足以覆蓋佇列首部封包大小時，即可出列發送
- 差額跨輪延續，確保長期位元組比例公平

## 倉庫結構

```
p4-cmcs/
├── my_project/                        # P4 程式與實驗腳本
│   ├── color_markers.p4
│   ├── topo/topology.json
│   ├── utils/
│   │   ├── run_exercise.py
│   │   └── p4runtime_switch.py
│   └── test/
│       ├── run_test.py
│       ├── plot_results.py
│       └── analyze_results.py
└── behavioral-model-patches/          # 修改後的 BMv2 原始碼
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

## 修改檔案說明

### my_project（P4 程式與實驗腳本）

| 檔案 | 說明 |
|---|---|
| `color_markers.p4` | 顏色標記器的 P4 實作：漏桶計量、VN 分類、metadata 寫入 |
| `utils/run_exercise.py` | 從拓樸配置檔讀取 `slice_weights`，並作為交換機啟動參數傳入 |
| `utils/p4runtime_switch.py` | 將 `--slice-weights w1,w2,...` 轉發至 `simple_switch_grpc` |
| `topo/topology.json` | 拓樸配置檔，包含 `"slice_weights": [...]` 欄位 |
| `test/run_test.py` | 多 VN 吞吐量實驗的主控腳本 |
| `test/plot_results.py` | 繪製逐秒吞吐量時序折線圖 |
| `test/analyze_results.py` | 分析誤差指標（均值、標準差、絕對誤差與相對誤差） |

### behavioral-model-patches（修改後的 BMv2 原始碼）

| 檔案 | 說明 |
|---|---|
| `include/bm/bm_sim/queueing.h` | 完整改寫為 `QueueingLogicVN`：在 `QE` 中新增 `pkt_size`；重新設計 `VNQueueInfo` / `LogicalQueueInfo` / `WorkerInfo`；四個 `push_front` 重載；`pop_back` 中的 bps 漏桶；兩階段 `select_vn`（RR + DWRR）；`set_rate` / `set_rate_for_all` 改為 bps 單位 |
| `targets/simple_switch/simple_switch.h` | 建構子參數改為 `vector<int> vn_weights`；新增 `nb_vns` 成員；`egress_buffers` 型別改為 `QueueingLogicVN`；定義 `SSWITCH_VN_QUEUEING_SRC` / `SSWITCH_COLOR_SRC` |
| `targets/simple_switch/simple_switch.cpp` | 更新建構子；`enqueue()` 中的超界截斷；容量介面對齊新方法名稱 |
| `targets/simple_switch/main.cpp` | 移除 `--priority-queues`；更新建構子呼叫，傳入 `{1}` |
| `targets/simple_switch/thrift/src/SimpleSwitch_server.cpp` | `set_egress_priority_queue_depth` 轉發至 `set_egress_vn_queue_depth`；`set_egress_priority_queue_rate` 存根回傳 0 |
| `targets/simple_switch_grpc/main.cpp` | 移除 `--priority-queues`；新增 `--slice-weights` 參數解析；未提供時預設為 `{1}` |
| `targets/simple_switch_grpc/switch_runner.h` | 參數改為 `std::vector<int> vn_weights` |
| `targets/simple_switch_grpc/switch_runner.cpp` | 更新建構子參數；轉發至 `SimpleSwitch` |

## 相依套件

- [BMv2](https://github.com/p4lang/behavioral-model)（commit：`392f801`）
- [p4lang/tutorials](https://github.com/p4lang/tutorials)（commit：`ce7d49f`）
- p4c `1.2.5.11`
- [Mininet](http://mininet.org/) `2.3.1b4`
- Python `3.12.3`
- iperf3
- tshark

## 復現方法

### 前置需求

請確保已安裝完整的 P4 開發環境，包含 `p4c`、`simple_switch_grpc` 與 Mininet。環境安裝方式請參考 [p4lang/tutorials](https://github.com/p4lang/tutorials) 的安裝腳本，各相依套件的確切版本請見[相依套件](#相依套件)一節。

### 1. 複製此倉庫

```bash
git clone https://github.com/<your-username>/p4-cmcs
cd p4-cmcs
```

### 2. 套用修補並重新編譯 BMv2

將修改後的原始碼複製至 BMv2 原始碼樹，再重新編譯並安裝：

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

### 3. 執行實驗並收集結果

```bash
cd <path-to-p4-cmcs>/my_project
make test
```

`make test` 將先建立虛擬網路拓樸並啟動 P4 交換機（相當於 `make run`），再自動執行測試腳本。實驗結果將儲存至 `my_project/test/result/`。

## 授權條款

本專案採用 **Apache License 2.0** 授權。詳情請參閱 [LICENSE](LICENSE) 文件。

`behavioral-model-patches/` 目錄下的檔案衍生自 [p4lang/behavioral-model](https://github.com/p4lang/behavioral-model)，同樣採用 Apache 2.0 授權。
Copyright 2013-present Barefoot Networks, Inc.