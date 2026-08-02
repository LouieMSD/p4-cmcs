/* Copyright 2013-present Barefoot Networks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Antonin Bas (antonin@barefootnetworks.com)
 *
 */

#ifndef SIMPLE_SWITCH_SIMPLE_SWITCH_H_
#define SIMPLE_SWITCH_SIMPLE_SWITCH_H_

#include <bm/bm_sim/queue.h>
#include <bm/bm_sim/queueing.h>
#include <bm/bm_sim/packet.h>
#include <bm/bm_sim/switch.h>
#include <bm/bm_sim/event_logger.h>
#include <bm/bm_sim/simple_pre_lag.h>

#include <memory>
#include <chrono>
#include <thread>
#include <vector>
#include <functional>

// Metadata field path used by the Traffic Manager to read the virtual network
// id (vn_id) written by the Ingress Pipeline color marker.
//
// After the Ingress Pipeline classifies a packet and performs the leaky-bucket
// conformance check, it writes the VN id into the user-defined metadata field
// meta.vn_id.  The Traffic Manager reads this value via the field path below
// to route the packet into the correct per-VN FIFO queue.  The path string
// must match the scalar name that the P4 compiler (p4c) assigns to the field
// in the compiled BMv2 JSON; verify against the JSON with:
//   grep -E "vn_id" build/prog.json | grep "scalars"
#define SSWITCH_VN_QUEUEING_SRC "scalars.metadata.vn_id"

// Metadata field path used by the Traffic Manager to read the color value
// written by the Ingress Pipeline color marker.
//
// The color marker assigns each packet one of two values: 0 (Green) if the
// packet falls within the VN's guaranteed bandwidth as determined by the
// leaky-bucket conformance check, or 1 (Yellow) if it exceeds the guaranteed
// rate.  The Traffic Manager reads this value via the field path below to
// decide whether to increment the Guaranteed Byte Credit (GB credit) counter
// for the corresponding VN queue on enqueue.  The path string must match the
// scalar name assigned by p4c; verify with:
//   grep -E "color" build/prog.json | grep "scalars"
#define SSWITCH_COLOR_SRC "scalars.metadata.color"

using ts_res = std::chrono::microseconds;
using std::chrono::duration_cast;
using ticks = std::chrono::nanoseconds;

using bm::Switch;
using bm::Queue;
using bm::Packet;
using bm::PHV;
using bm::Parser;
using bm::Deparser;
using bm::Pipeline;
using bm::McSimplePreLAG;
using bm::Field;
using bm::FieldList;
using bm::packet_id_t;
using bm::p4object_id_t;

class SimpleSwitch : public Switch {
 public:
  using mirror_id_t = int;

  using TransmitFn = std::function<void(port_t, packet_id_t,
                                        const char *, int)>;

  struct MirroringSessionConfig {
    port_t egress_port;
    bool egress_port_valid;
    unsigned int mgid;
    bool mgid_valid;
  };

  static constexpr port_t default_drop_port = 511;

  // The number of virtual networks is no longer a compile-time scalar constant.
  // It is inferred at runtime from vn_weights.size() and stored in nb_vns, so
  // that the scheduler can be configured with an arbitrary number of VNs by
  // supplying the appropriate weight vector at construction time.
  static constexpr int default_mgid_table_size =
      bm::McSimplePre::DEFAULT_MGID_TABLE_SIZE;
  static constexpr int default_l1_max_entries =
      bm::McSimplePre::DEFAULT_L1_MAX_ENTRIES;
  static constexpr int default_l2_max_entries =
      bm::McSimplePre::DEFAULT_L2_MAX_ENTRIES;

 private:
  using clock = std::chrono::high_resolution_clock;

 public:
  // Construct a SimpleSwitch instance with CMCS traffic management.
  //
  // vn_weights defines the number of virtual networks k = vn_weights.size()
  // and the Deficit Weighted Round Robin (DWRR) sharing weight w_i for each
  // VN i.  These weights govern how residual link capacity is distributed among
  // VNs whose traffic demand exceeds their guaranteed bandwidth allocation.
  // When omitted, the switch defaults to a single VN with weight 1.
  explicit SimpleSwitch(bool enable_swap = false,
                        port_t drop_port = default_drop_port,
                        std::vector<int> vn_weights = {1},
                        int mgid_table_size = default_mgid_table_size,
                        int l1_max_entries = default_l1_max_entries,
                        int l2_max_entries = default_l2_max_entries);

  ~SimpleSwitch();

  int receive_(port_t port_num, const char *buffer, int len) override;

  void start_and_return_() override;

  void reset_target_state_() override;

  void swap_notify_() override;

  bool mirroring_add_session(mirror_id_t mirror_id,
                             const MirroringSessionConfig &config);

  bool mirroring_delete_session(mirror_id_t mirror_id);

  bool mirroring_get_session(mirror_id_t mirror_id,
                             MirroringSessionConfig *config) const;

  // Set the maximum queue depth (in packets) for VN vn_id on egress port port.
  // Packets arriving when the VN queue has reached this depth are tail-dropped.
  // This provides per-VN buffer isolation so that a bursty VN cannot exhaust
  // the shared packet buffer and starve other VNs on the same port.
  int set_egress_vn_queue_depth(size_t port, size_t vn_id,
                                  const size_t depth_pkts);

  // Set the queue depth for all VNs on egress port port to depth_pkts.
  int set_egress_queue_depth(size_t port, const size_t depth_pkts);

  // Set the queue depth for all VNs on all egress ports to depth_pkts.
  int set_all_egress_queue_depths(const size_t depth_pkts);

  // Set the dequeue rate limit for egress port port to rate_pps bits per
  // second.  Rate limiting is enforced by a per-port leaky bucket in the
  // Traffic Manager and models the physical link capacity constraint.
  // Pass 0 to disable rate limiting on the port.
  int set_egress_queue_rate(size_t port, const uint64_t rate_pps);

  // Set the dequeue rate limit for all egress ports to rate_pps bits per
  // second.
  int set_all_egress_queue_rates(const uint64_t rate_pps);

  // returns the number of microseconds elapsed since the switch started
  uint64_t get_time_elapsed_us() const;

  // returns the number of microseconds elasped since the clock's epoch
  uint64_t get_time_since_epoch_us() const;

  // returns the packet id of most recently received packet. Not thread-safe.
  static packet_id_t get_packet_id() {
    return packet_id - 1;
  }

  void set_transmit_fn(TransmitFn fn);

  port_t get_drop_port() const {
    return drop_port;
  }

  SimpleSwitch(const SimpleSwitch &) = delete;
  SimpleSwitch &operator =(const SimpleSwitch &) = delete;
  SimpleSwitch(SimpleSwitch &&) = delete;
  SimpleSwitch &&operator =(SimpleSwitch &&) = delete;

 private:
  static constexpr size_t nb_egress_threads = 4u;
  static packet_id_t packet_id;

  class MirroringSessions;

  class InputBuffer;

  enum PktInstanceType {
    PKT_INSTANCE_TYPE_NORMAL,
    PKT_INSTANCE_TYPE_INGRESS_CLONE,
    PKT_INSTANCE_TYPE_EGRESS_CLONE,
    PKT_INSTANCE_TYPE_COALESCED,
    PKT_INSTANCE_TYPE_RECIRC,
    PKT_INSTANCE_TYPE_REPLICATION,
    PKT_INSTANCE_TYPE_RESUBMIT,
  };

  struct EgressThreadMapper {
    explicit EgressThreadMapper(size_t nb_threads)
        : nb_threads(nb_threads) { }

    size_t operator()(size_t egress_port) const {
      return egress_port % nb_threads;
    }

    size_t nb_threads;
  };

 private:
  void ingress_thread();
  void egress_thread(size_t worker_id);
  void transmit_thread();

  ts_res get_ts() const;

  void enqueue(port_t egress_port, std::unique_ptr<Packet> &&packet);

  void copy_field_list_and_set_type(
      const std::unique_ptr<Packet> &packet,
      const std::unique_ptr<Packet> &packet_copy,
      PktInstanceType copy_type, p4object_id_t field_list_id);

  void check_queueing_metadata();

  void multicast(Packet *packet, unsigned int mgid);

 private:
  port_t drop_port;
  std::vector<std::thread> threads_;
  std::unique_ptr<InputBuffer> input_buffer;

  // Cached number of virtual networks, equal to vn_weights.size() at
  // construction time.  Stored here so that enqueue() can perform an O(1)
  // bounds check on the vn_id read from packet metadata without querying the
  // egress_buffers internals on every packet.
  size_t nb_vns;

  // Central egress buffer pool implementing the CMCS Traffic Manager.
  //
  // Replaces the original BMv2 QueueingLogicPriRL (strict-priority queue with
  // packet-rate limiting).  Each egress port maintains one independent FIFO
  // queue per virtual network; packets within the same VN are always served in
  // arrival order, eliminating intra-VN reordering.  The two-phase CMCS
  // scheduling algorithm (GB-credit priority service followed by DWRR residual
  // sharing) is implemented inside QueueingLogicVN and is invoked each time a
  // transmission opportunity arises on an egress port.
  bm::QueueingLogicVN<std::unique_ptr<Packet>, EgressThreadMapper>
  egress_buffers;

  Queue<std::unique_ptr<Packet> > output_buffer;
  TransmitFn my_transmit_fn;
  std::shared_ptr<McSimplePreLAG> pre;
  clock::time_point start;
  bool with_queueing_metadata{false};
  std::unique_ptr<MirroringSessions> mirroring_sessions;
};

#endif  // SIMPLE_SWITCH_SIMPLE_SWITCH_H_