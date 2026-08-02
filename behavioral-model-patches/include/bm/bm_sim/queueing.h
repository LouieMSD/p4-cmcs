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

//! @file queueing.h
//! This file contains convenience classes that can be useful for targets that
//! wish to queue packets at some point during processing (for example, between
//! an ingress pipeline and an egress pipeline, as is the case for the standard
//! simple switch target). We realized that if one decided to use the bm::Queue
//! class (in queue.h) to achieve this, quite a lot of work was required, even
//! for the standard, basic case: one queue per egress port, with a limited
//! number of threads processing all the queues.

#ifndef BM_BM_SIM_QUEUEING_H_
#define BM_BM_SIM_QUEUEING_H_

#include <algorithm>  // for std::max
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <tuple>  // for std::forward_as_tuple
#include <unordered_map>
#include <utility>  // for std::piecewise_construct
#include <vector>

namespace bm {

// These queueing implementations used to have one lock for each worker, which
// meant that as long as 2 queues were assigned to different workers, they could
// operate (push / pop) in parallel. Since we added support for arbitrary port
// ids (and the port id is used as the queue id), we no longer have a reasonable
// upper bound on the maximum possible port id at construction time and we can
// no longer use a vector indexed by the queue id to store queue
// information. Each push / pop operation can potentially insert a new entry
// into the map. In order to accomodate for this, we had to start using a single
// lock, shared by all the workers. It's unlikely that contention for this lock
// will be a bottleneck.

//! One of the most basic queueing block possible. Supports an arbitrary number
//! of logical queues (identified by arbitrary integer ids). Lets you choose (at
//! runtime) the number of worker threads that will be reading from these
//! queues. I write "logical queues" because the implementation actually uses as
//! many physical queues as there are worker threads. However, each logical
//! queue still has its own maximum capacity.  As of now, the behavior is
//! blocking for both read (pop_back()) and write (push_front()), but we may
//! offer additional options if there is interest expressed in the future.
//!
//! Template parameter `T` is the type (has to be movable) of the objects that
//! will be stored in the queues. Template parameter `FMap` is a callable object
//! that has to be able to map every logical queue id to a worker id. The
//! following is a good example of functor that meets the requirements:
//! @code
//! struct WorkerMapper {
//!   WorkerMapper(size_t nb_workers)
//!       : nb_workers(nb_workers) { }
//!
//!   size_t operator()(size_t queue_id) const {
//!     return queue_id % nb_workers;
//!   }
//!
//!   size_t nb_workers;
//! };
//! @endcode
template <typename T, typename FMap>
class QueueingLogic {
  using MutexType = std::mutex;
  using LockType = std::unique_lock<MutexType>;

 public:
  //! \p nb_workers is the number of threads that will be consuming from the
  //! queues; they will be identified by an id in the range `[0,
  //! nb_workers)`. \p capacity is the number of objects that each logical queue
  //! can hold. Because we need to be able to map each queue id to a worker id,
  //! the user has to provide a callable object of type `FMap`, \p
  //! map_to_worker, that can do this mapping. See the QueueingLogic class
  //! description for more information about the `FMap` template parameter.
  QueueingLogic(size_t nb_workers, size_t capacity, FMap map_to_worker)
      : nb_workers(nb_workers),
        capacity(capacity),
        workers_info(nb_workers),
        map_to_worker(std::move(map_to_worker)) { }

  //! Makes a copy of \p item and pushes it to the front of the logical queue
  //! with id \p queue_id.
  void push_front(size_t queue_id, const T &item) {
    size_t worker_id = map_to_worker(queue_id);
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    auto &w_info = workers_info.at(worker_id);
    while (q_info.size >= q_info.capacity) {
      q_info.q_not_full.wait(lock);
    }
    w_info.queue.emplace_front(item, queue_id);
    q_info.size++;
    w_info.q_not_empty.notify_one();
  }

  //! Moves \p item to the front of the logical queue with id \p queue_id.
  void push_front(size_t queue_id, T &&item) {
    size_t worker_id = map_to_worker(queue_id);
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    auto &w_info = workers_info.at(worker_id);
    while (q_info.size >= q_info.capacity) {
      q_info.q_not_full.wait(lock);
    }
    w_info.queue.emplace_front(std::move(item), queue_id);
    q_info.size++;
    w_info.q_not_empty.notify_one();
  }

  //! Retrieves the oldest element for the worker thread indentified by \p
  //! worker_id and moves it to \p pItem. The id of the logical queue which
  //! contained this element is copied to \p queue_id. As a remainder, the
  //! `map_to_worker` argument provided when constructing the class is used to
  //! map every queue id to the corresponding worker id. Therefore, if an
  //! element `E` was pushed to queue `queue_id`, you need to use the worker id
  //! `map_to_worker(queue_id)` to retrieve it with this function.
  void pop_back(size_t worker_id, size_t *queue_id, T *pItem) {
    LockType lock(mutex);
    auto &w_info = workers_info.at(worker_id);
    auto &queue = w_info.queue;
    while (queue.size() == 0) {
      w_info.q_not_empty.wait(lock);
    }
    *queue_id = queue.back().queue_id;
    *pItem = std::move(queue.back().e);
    queue.pop_back();
    auto &q_info = get_queue_or_throw(*queue_id);
    q_info.size--;
    q_info.q_not_full.notify_one();
  }

  //! Get the occupancy of the logical queue with id \p queue_id.
  size_t size(size_t queue_id) const {
    LockType lock(mutex);
    auto it = queues_info.find(queue_id);
    if (it == queues_info.end()) return 0;
    auto &q_info = it->second;
    return q_info.size;
  }

  //! Set the capacity of the logical queue with id \p queue_id to \p c
  //! elements.
  void set_capacity(size_t queue_id, size_t c) {
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    q_info.capacity = c;
  }

  //! Set the capacity of all logical queues to \p c elements.
  void set_capacity_for_all(size_t c) {
    LockType lock(mutex);
    for (auto &p : queues_info) p.second.capacity = c;
    capacity = c;
  }

  //! Deleted copy constructor
  QueueingLogic(const QueueingLogic &) = delete;
  //! Deleted copy assignment operator
  QueueingLogic &operator =(const QueueingLogic &) = delete;

  //! Deleted move constructor
  QueueingLogic(QueueingLogic &&) = delete;
  //! Deleted move assignment operator
  QueueingLogic &&operator =(QueueingLogic &&) = delete;

 private:
  struct QE {
    QE(T e, size_t queue_id)
        : e(std::move(e)), queue_id(queue_id) { }

    T e;
    size_t queue_id;
  };

  using MyQ = std::deque<QE>;

  struct QueueInfo {
    explicit QueueInfo(size_t capacity)
        : capacity(capacity) { }

    size_t size{0};
    size_t capacity{0};
    mutable std::condition_variable q_not_full{};
  };

  struct WorkerInfo {
    MyQ queue{};
    mutable std::condition_variable q_not_empty{};
  };

  QueueInfo &get_queue(size_t queue_id) {
    auto it = queues_info.find(queue_id);
    if (it != queues_info.end()) return it->second;
    // piecewise_construct because QueueInfo is not copyable (because of mutex
    // member)
    auto p = queues_info.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(queue_id),
        std::forward_as_tuple(capacity));
    return p.first->second;
  }

  const QueueInfo &get_queue_or_throw(size_t queue_id) const {
    return queues_info.at(queue_id);
  }

  QueueInfo &get_queue_or_throw(size_t queue_id) {
    return queues_info.at(queue_id);
  }

  mutable MutexType mutex{};
  size_t nb_workers;
  size_t capacity;  // default capacity
  std::unordered_map<size_t, QueueInfo> queues_info;
  std::vector<WorkerInfo> workers_info;
  FMap map_to_worker;
};


//! This class is slightly more advanced than QueueingLogic. The difference
//! between the 2 is that this one offers the ability to rate-limit every
//! logical queue, by providing a maximum number of elements consumed per
//! second. If the rate is too small compared to the incoming packet rate, or if
//! the worker thread cannot sustain the desired rate, elements are buffered in
//! the queue. However, the write behavior (push_front()) for this class is
//! different than the one for QueueingLogic. It is not blocking: if the queue
//! is full, the function will return immediately and the element will not be
//! queued. Look at the documentation for QueueingLogic for more information
//! about the template parameters (they are the same).
//! This is the queueing logic used by the standard simple_switch target.
template <typename T, typename FMap>
class QueueingLogicRL {
  using MutexType = std::mutex;
  using LockType = std::unique_lock<MutexType>;

 public:
  //! @copydoc QueueingLogic::QueueingLogic()
  //!
  //! Initially, none of the logical queues will be rate-limited, i.e. the
  //! instance will behave as an instance of QueueingLogic.
  QueueingLogicRL(size_t nb_workers, size_t capacity, FMap map_to_worker)
      : nb_workers(nb_workers),
        capacity(capacity),
        workers_info(nb_workers),
        map_to_worker(std::move(map_to_worker)) { }

  //! If the logical queue with id \p queue_id is full, the function will return
  //! `0` immediately. Otherwise, \p item will be copied to the front of the
  //! logical queue and the function will return `1`.
  int push_front(size_t queue_id, const T &item) {
    size_t worker_id = map_to_worker(queue_id);
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    auto &w_info = workers_info.at(worker_id);
    if (q_info.size >= q_info.capacity) return 0;
    q_info.last_sent = get_next_tp(q_info);
    w_info.queue.emplace(
        item, queue_id, q_info.last_sent, w_info.wrapping_counter++);
    q_info.size++;
    w_info.q_not_empty.notify_one();
    return 1;
  }

  //! Same as push_front(size_t queue_id, const T &item), but \p item is moved
  //! instead of copied.
  int push_front(size_t queue_id, T &&item) {
    size_t worker_id = map_to_worker(queue_id);
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    auto &w_info = workers_info.at(worker_id);
    if (q_info.size >= q_info.capacity) return 0;
    q_info.last_sent = get_next_tp(q_info);
    w_info.queue.emplace(
        std::move(item), queue_id, q_info.last_sent, w_info.wrapping_counter++);
    q_info.size++;
    w_info.q_not_empty.notify_one();
    return 1;
  }

  //! Retrieves the oldest element for the worker thread indentified by \p
  //! worker_id and moves it to \p pItem. The id of the logical queue which
  //! contained this element is copied to \p queue_id. Note that this function
  //! will block until 1) an element is available 2) this element is free to
  //! leave the queue according to the rate limiter.
  void pop_back(size_t worker_id, size_t *queue_id, T *pItem) {
    LockType lock(mutex);
    auto &w_info = workers_info.at(worker_id);
    auto &queue = w_info.queue;
    while (true) {
      if (queue.size() == 0) {
        w_info.q_not_empty.wait(lock);
      } else {
        if (queue.top().send <= clock::now()) break;
        w_info.q_not_empty.wait_until(lock, queue.top().send);
      }
    }
    *queue_id = queue.top().queue_id;
    // TODO(antonin): improve / document this
    // http://stackoverflow.com/questions/20149471/move-out-element-of-std-priority-queue-in-c11
    *pItem = std::move(const_cast<QE &>(queue.top()).e);
    queue.pop();
    auto &q_info = get_queue_or_throw(*queue_id);
    q_info.size--;
  }

  //! @copydoc QueueingLogic::size
  size_t size(size_t queue_id) const {
    LockType lock(mutex);
    auto it = queues_info.find(queue_id);
    if (it == queues_info.end()) return 0;
    auto &q_info = it->second;
    return q_info.size;
  }

  //! Set the capacity of the logical queue with id \p queue_id to \p c
  //! elements.
  void set_capacity(size_t queue_id, size_t c) {
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    q_info.capacity = c;
  }

  //! Set the capacity of all logical queues to \p c elements.
  void set_capacity_for_all(size_t c) {
    LockType lock(mutex);
    for (auto &p : queues_info) p.second.capacity = c;
    capacity = c;
  }

  //! Set the maximum rate of the logical queue with id \p queue_id to \p
  //! pps. \p pps is expressed in "number of elements per second". Until this
  //! function is called, there will be no rate limit for the queue. The same
  //! behavior (no rate limit) can be achieved by calling this method with a
  //! rate of 0.
  void set_rate(size_t queue_id, uint64_t pps) {
    LockType lock(mutex);
    auto &q_info = get_queue(queue_id);
    q_info.queue_rate_pps = pps;
    q_info.pkt_delay_ticks = rate_to_ticks(pps);
  }

  //! Set the maximum rate of all logical queues to \p pps.
  void set_rate_for_all(uint64_t pps) {
    using std::chrono::duration;
    using std::chrono::duration_cast;
    LockType lock(mutex);
    for (auto &p : queues_info) {
      auto &q_info = p.second;
      q_info.queue_rate_pps = pps;
      q_info.pkt_delay_ticks = rate_to_ticks(pps);
    }
    queue_rate_pps = pps;
  }

  //! Deleted copy constructor
  QueueingLogicRL(const QueueingLogicRL &) = delete;
  //! Deleted copy assignment operator
  QueueingLogicRL &operator =(const QueueingLogicRL &) = delete;

  //! Deleted move constructor
  QueueingLogicRL(QueueingLogicRL &&) = delete;
  //! Deleted move assignment operator
  QueueingLogicRL &&operator =(QueueingLogicRL &&) = delete;

 private:
  using ticks = std::chrono::nanoseconds;
  // clock choice? switch to steady if observing re-ordering
  // using clock = std::chrono::steady_clock;
  using clock = std::chrono::high_resolution_clock;

  static constexpr ticks rate_to_ticks(uint64_t pps) {
    using std::chrono::duration;
    using std::chrono::duration_cast;
    return (pps == 0) ?
        ticks(0) : duration_cast<ticks>(duration<double>(1. / pps));
  }

  struct QE {
    // QE(T e, size_t queue_id, const clock::time_point &send, size_t id)
    //     : e(std::move(e)), queue_id(queue_id), send(send), id(id) { }
    QE(T e, size_t queue_id, const clock::time_point &send, size_t id)
        : e(std::move(e)), queue_id(queue_id), send(send), id(id) { }

    T e;
    size_t queue_id;
    clock::time_point send;
    size_t id;
  };

  struct QEComp {
    bool operator()(const QE &lhs, const QE &rhs) const {
      // the point of the id is to avoid re-orderings when the send timestamp is
      // the same for 2 items, which seems to happen (when the pps rate is 0)
      // with both the steady_clock and the high_resolution_clock on my Linux
      // VM.
      return (lhs.send == rhs.send) ? lhs.id > rhs.id : lhs.send > rhs.send;
    }
  };

  // performance seems to be roughly the same for deque vs vector
  using MyQ = std::priority_queue<QE, std::deque<QE>, QEComp>;
  // using MyQ = std::priority_queue<QE, std::vector<QE>, QEComp>;

  struct QueueInfo {
    QueueInfo(size_t capacity, uint64_t queue_rate_pps)
        : capacity(capacity),
          queue_rate_pps(queue_rate_pps),
          pkt_delay_ticks(rate_to_ticks(queue_rate_pps)),
          last_sent(clock::now()) { }

    size_t size{0};
    size_t capacity;
    uint64_t queue_rate_pps;
    ticks pkt_delay_ticks;
    clock::time_point last_sent;
  };

  struct WorkerInfo {
    MyQ queue{};
    mutable std::condition_variable q_not_empty{};
    size_t wrapping_counter{0};
  };

  QueueInfo &get_queue(size_t queue_id) {
    auto it = queues_info.find(queue_id);
    if (it != queues_info.end()) return it->second;
    auto p = queues_info.emplace(queue_id, QueueInfo(capacity, queue_rate_pps));
    return p.first->second;
  }

  const QueueInfo &get_queue_or_throw(size_t queue_id) const {
    return queues_info.at(queue_id);
  }

  QueueInfo &get_queue_or_throw(size_t queue_id) {
    return queues_info.at(queue_id);
  }

  clock::time_point get_next_tp(const QueueInfo &q_info) {
    return std::max(clock::now(), q_info.last_sent + q_info.pkt_delay_ticks);
  }

  mutable MutexType mutex{};
  size_t nb_workers;
  size_t capacity;  // default capacity
  uint64_t queue_rate_pps{0};  // default rate
  std::unordered_map<size_t, QueueInfo> queues_info;
  std::vector<WorkerInfo> workers_info;
  FMap map_to_worker;
};


// Virtual-network-aware packet scheduler implementing the Color Marker and
// Color-Aware Scheduler (CMCS) queuing logic.
//
// This class replaces the original BMv2 QueueingLogicPriRL (strict-priority
// queue with rate limiting) with a scheduler that treats each virtual network
// (VN) as an independent scheduling dimension.  Each logical queue (egress
// port) maintains one FIFO deque per VN; packets within the same VN are always
// served in arrival order, eliminating intra-VN reordering.
//
// Scheduling proceeds in two phases whenever a transmission opportunity arises:
//
//   Phase 1 – Guaranteed-bandwidth service (Round Robin over positive GB
//   credits).  Each green packet enqueued increments the VN's Guaranteed Byte
//   Credit (GB credit) counter by its byte length.  The scheduler scans the
//   active set in round-robin order and selects the first VN whose GB credit is
//   positive and whose queue is non-empty, then deducts the head-of-line packet
//   size from that credit.  Credits may go negative (surplus semantics), so
//   that a large head-of-line packet does not strand accumulated credit across
//   scheduling rounds.
//
//   Phase 2 – Residual-capacity sharing (Deficit Weighted Round Robin).  When
//   no VN in the active set holds a positive GB credit, the scheduler falls
//   back to DWRR.  Each VN accumulates a weighted_deficit at a rate
//   proportional to its sharing weight w_i per round; the scheduler selects the
//   first non-empty VN with a positive weighted_deficit and deducts the
//   head-of-line packet size.  Deficits carry over across rounds and may go
//   negative, ensuring that the long-run byte throughput ratio converges to the
//   configured weight ratios w_1 : w_2 : ... : w_k.
//
// Dequeue rate limiting is implemented as a per-logical-queue leaky-bucket
// token model in bits per second (bps), rather than a fixed packet-interval
// (pps) model, so that variable-length packets are accounted for correctly.
//
// The active set tracks which VNs are currently competing for transmission.
// A VN is added to the active set on its first enqueue; it is removed after
// remaining continuously empty for longer than vn_idle_timeout, at which point
// its weighted_deficit is reset to zero to prevent a burst of stale credit from
// monopolising the link upon reactivation.
template <typename T, typename FMap>
class QueueingLogicVN {
  using MutexType = std::mutex;
  using LockType = std::unique_lock<MutexType>;

 public:
  // Construct a VN-aware scheduler.
  //
  // vn_weights defines both the number of virtual networks k = vn_weights.size()
  // and the DWRR sharing weight w_i for each VN i.  When omitted, the scheduler
  // defaults to a single VN with weight 1, behaving like an ordinary
  // rate-limited FIFO queue.
  QueueingLogicVN(size_t nb_workers, size_t capacity,
                     FMap map_to_worker,
                     std::vector<int> vn_weights = {1})
      : nb_workers(nb_workers),
        capacity(capacity),
        vn_weights(std::move(vn_weights)),
        nb_vns(this->vn_weights.size()),
        workers_info(nb_workers),
        map_to_worker(std::move(map_to_worker)) { }

  // Enqueue packet item into the FIFO queue of VN vn_queue_id under logical
  // queue logical_queue_id.
  //
  // Two ordering constraints must be observed:
  //
  //   1. GB credit accumulation (green_credit += pkt_size) is performed before
  //      the capacity check.  If the packet is subsequently dropped because the
  //      VN queue is full, the credit already recorded remains valid and will be
  //      consumed by Phase 1 scheduling on behalf of future packets.  Tying
  //      credit accumulation to successful enqueue would cause guaranteed
  //      bandwidth to be silently lost under congestion.
  //
  //   2. The active-set membership check is performed before incrementing
  //      sq.size.  The check relies on detecting the transition from empty to
  //      non-empty; if size were incremented first, that transition would be
  //      invisible and the VN would never be added to the active set.
  //
  // pkt_size is read from the packet before any move, because sentinel packets
  // (used to wake worker threads on shutdown) may carry a null pointer; those
  // are enqueued as push_front(i, 0, 0, nullptr) and must not be dereferenced.
  //
  // Returns 1 on success; returns 0 (no blocking) if the VN queue has reached
  // its capacity limit.
  int push_front(size_t logical_queue_id,
                size_t vn_queue_id,
                size_t color, const T &item) {
    size_t worker_id = map_to_worker(logical_queue_id);
    LockType lock(mutex);
    auto &w_info = workers_info[worker_id];

    auto &lq = get_logical_queue(logical_queue_id, w_info);
    auto &sq = lq.vn_queues[vn_queue_id];

    size_t pkt_size = item ? item->get_ingress_length() : 0;

    // Accumulate GB credit for green packets before the capacity check so that
    // credit is not lost even if this packet is dropped due to queue overflow.
    if (color == 0) sq.green_credit += (int)pkt_size;

    if (sq.size >= sq.capacity) return 0;

    // Add vn_queue_id to the active set if not already present.  A linear
    // search suffices because nb_vns is small; the result is appended to the
    // end of active_set so that newly (re)activated VNs join the tail of the
    // scheduling order rather than resuming their original position.
    if (std::find(lq.active_set.begin(), lq.active_set.end(), vn_queue_id)
            == lq.active_set.end()) {
      lq.active_set.push_back(vn_queue_id);
    }

    w_info.physical_queues[logical_queue_id][vn_queue_id].push_back(
        QE(item, color, pkt_size));

    sq.size++;
    lq.size++;
    w_info.size++;
    w_info.q_not_empty.notify_one();
    return 1;
  }

  // Convenience overload that omits color; defaults to Yellow (1), which does
  // not trigger GB credit accumulation.
  int push_front(size_t logical_queue_id, size_t vn_queue_id,
                const T &item) {
    return push_front(logical_queue_id, vn_queue_id, 1, item);
  }

  // Move-semantic overload of push_front.  Logic is identical to the
  // const-reference version; pkt_size must be read before std::move(item) is
  // called, because the packet object is in an unspecified state after the move.
  int push_front(size_t logical_queue_id, size_t vn_queue_id,
                size_t color, T &&item) {
    size_t worker_id = map_to_worker(logical_queue_id);
    LockType lock(mutex);

    auto &w_info = workers_info[worker_id];
    auto &lq = get_logical_queue(logical_queue_id, w_info);
    auto &sq = lq.vn_queues[vn_queue_id];

    // Read packet size before moving the item, as the object becomes
    // unspecified after std::move.
    size_t pkt_size = item ? item->get_ingress_length() : 0;

    if (color == 0) sq.green_credit += (int)pkt_size;

    if (sq.size >= sq.capacity) return 0;

    if (std::find(lq.active_set.begin(), lq.active_set.end(), vn_queue_id)
            == lq.active_set.end()) {
      lq.active_set.push_back(vn_queue_id);
    }

    w_info.physical_queues[logical_queue_id][vn_queue_id].push_back(
        QE(std::move(item), color, pkt_size));

    sq.size++;
    lq.size++;
    w_info.size++;
    w_info.q_not_empty.notify_one();
    return 1;
  }

  // Convenience move overload that omits color; defaults to Yellow (1).
  int push_front(size_t logical_queue_id, size_t vn_queue_id,
                T &&item) {
    return push_front(logical_queue_id, vn_queue_id, 1, std::move(item));
  }

  // Dequeue one packet from worker worker_id and write its logical queue id,
  // VN id, color, and packet body to the output parameters.
  //
  // The dequeue decision proceeds through three nested layers:
  //
  //   Layer 1 – Worker layer.  If the total packet count across all logical
  //   queues owned by this worker is zero, the thread blocks on a condition
  //   variable until a new packet arrives.
  //
  //   Layer 2 – Logical-queue layer.  Starting from worker_rr_ptr, the
  //   scheduler scans logical queues in round-robin order.  For each non-empty
  //   candidate, it checks whether the dequeue leaky bucket allows a packet of
  //   the chosen VN's head-of-line size to leave.  If the bucket rejects the
  //   candidate, the scheduler records the earliest future release time and
  //   moves on to the next logical queue; if no logical queue can be served
  //   immediately, the thread sleeps until the earliest release time.
  //   worker_rr_ptr is advanced only when a packet is actually dequeued,
  //   preserving round-robin fairness regardless of how many candidates were
  //   skipped.
  //
  //   Layer 3 – VN layer.  For the selected logical queue, select_vn() runs
  //   the two-phase CMCS scheduling algorithm over the active set.
  //
  // The leaky bucket operates in bps (bits per second) rather than pps, so
  // that variable-length packets are metered accurately against the configured
  // link rate.
  void pop_back(size_t worker_id, size_t *logical_queue_id,
                size_t *vn_queue_id, size_t *color, T *pItem) {
    LockType lock(mutex);
    auto &w_info = workers_info[worker_id];

    while (true) {
      if (w_info.size == 0) {
        w_info.q_not_empty.wait(lock);
        continue;
      }

      // next_wake_up_time accumulates the earliest release time across all
      // logical queues that are blocked by the dequeue leaky bucket in this
      // scan round.  If no logical queue can be served immediately, the thread
      // sleeps until that time to avoid busy-waiting while still reacting as
      // promptly as possible.
      auto now = clock::now();
      auto next_wake_up_time = clock::time_point::max();
      size_t n = w_info.logical_queue_ids.size();
      bool selected = false;
      size_t selected_qid = 0;
      size_t selected_vn = 0;

      for (size_t k = 0; k < n; k++) {
        size_t j = (w_info.worker_rr_ptr + k) % n;
        size_t qid = w_info.logical_queue_ids[j];
        auto &lq = logical_queues_info.find(qid)->second;
        auto &phys = w_info.physical_queues[qid];

        if (lq.size == 0) continue;

        // Determine the VN to serve from this logical queue.
        //
        // If pending_vn is set, a previous call to select_vn() already produced
        // a scheduling decision that was blocked by the dequeue leaky bucket.
        // Reuse that decision directly to avoid re-running select_vn(), which
        // would advance scheduling state a second time for the same packet and
        // could select a different VN, corrupting GB credits and DWRR deficits.
        // The idle-timeout sweep is also skipped when pending_vn is set, because
        // the pending VN must not be evicted from the active set while its
        // transmission is merely deferred by rate limiting.
        size_t sel;
        if (lq.pending_vn >= 0) {
            sel = (size_t)lq.pending_vn;
        } else {
            // Idle-timeout sweep: remove VNs that have been continuously empty
            // for longer than vn_idle_timeout from the active set and reset
            // their weighted_deficit to zero.  Resetting the deficit prevents a
            // long-idle VN from monopolising the link upon reactivation due to
            // accumulated but unconsumed credit.  GB credit (green_credit) is
            // intentionally preserved, because it represents guaranteed
            // bandwidth that has already been metered in by the color marker and
            // should remain redeemable when the VN becomes active again.
            for (auto it = lq.active_set.begin(); it != lq.active_set.end(); ) {
              size_t i  = *it;
              auto  &sq = lq.vn_queues[i];

              if (sq.size == 0 && (now - sq.empty_since) >= vn_idle_timeout) {
                sq.weighted_deficit = 0;
                it = lq.active_set.erase(it);
                continue;
              }
              ++it;
            }
            sel = select_vn(lq, phys);
            lq.pending_vn = (int)sel;
        }

        // Dequeue leaky-bucket rate check.
        //
        // The virtual water level X is decayed by elapsed * dequeue_rate_bps
        // before testing against the burst ceiling tau.  X' is not clamped at
        // zero; a negative value indicates that the bucket is drained and there
        // is residual idle capacity available, which makes the rate limiter
        // smoother under low load by absorbing brief early arrivals without
        // immediately triggering a wait.
        //
        // If dequeue_rate_bps == 0 the bucket is always considered compliant
        // (no rate limiting).
        double T_bits  = (double)phys[sel].front().pkt_size * 8.0;
        double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(
                            now - lq.LCT).count();
        double X_prime = lq.X - elapsed * lq.dequeue_rate_bps;

        if (X_prime > lq.tau) {
          // Rate limit exceeded: compute the earliest time at which the water
          // level will fall back to tau and record it in next_wake_up_time.
          // pending_vn is left intact so the cached VN decision is reused on
          // the next attempt.
          double wait_secs  = (X_prime - lq.tau) / lq.dequeue_rate_bps;
          auto   wake_time  = now + std::chrono::duration_cast<clock::duration>(
                                  std::chrono::duration<double>(wait_secs));
          next_wake_up_time = std::min(next_wake_up_time, wake_time);
          continue;
        } else {
          // Compliant: update the bucket water level immediately before leaving
          // the scan loop so that a subsequent iteration over the same logical
          // queue in the same round sees the updated level.
          lq.X          = std::max(0.0, X_prime) + T_bits;
          lq.LCT        = now;
          lq.pending_vn = -1;
          selected      = true;
          selected_qid  = qid;
          selected_vn   = sel;
          // Advance worker_rr_ptr to the position after the queue that just
          // produced a packet, so the next pop_back call begins scanning from
          // there, maintaining round-robin fairness independently of how many
          // queues were skipped in this round.
          w_info.worker_rr_ptr = (j + 1) % n;
          break;
        }
      }

      if (!selected) {
        // All logical queues were blocked by their dequeue leaky buckets.
        // Sleep until the earliest projected release time rather than
        // busy-waiting or using a fixed polling interval.
        w_info.q_not_empty.wait_until(lock, next_wake_up_time);
        continue;
      }

      // Re-acquire the LogicalQueueInfo reference using the selected queue id
      // recorded above, since the loop-local lq reference is no longer in scope.
      auto &lq = logical_queues_info.find(selected_qid)->second;

      // Dequeue the head-of-line packet from the selected VN's FIFO deque.
      // Intra-VN ordering is strictly preserved because packets are always
      // inserted at the tail and removed from the head.
      auto &dq = w_info.physical_queues[selected_qid][selected_vn];
      QE qe = std::move(dq.front());
      dq.pop_front();

      *logical_queue_id = selected_qid;
      *vn_queue_id      = selected_vn;
      *color            = qe.color;
      *pItem            = std::move(qe.e);

      lq.vn_queues[selected_vn].size--;
      if (lq.vn_queues[selected_vn].size == 0) {
        // Record the time at which this VN became empty so the idle-timeout
        // sweep in a future pop_back call can determine whether it has been
        // idle long enough to be removed from the active set.  Reusing the
        // already-computed now avoids an extra clock::now() call and keeps
        // the timestamp consistent with the leaky-bucket elapsed calculation.
        lq.vn_queues[selected_vn].empty_since = now;
      }
      lq.size--;
      w_info.size--;

      break;
    }
  }

  // Convenience overload that discards the vn_queue_id and color outputs.
  void pop_back(size_t worker_id, size_t *logical_queue_id, T *pItem) {
    size_t vn_queue_id, color;
    return pop_back(worker_id, logical_queue_id, &vn_queue_id, &color, pItem);
  }

  // Returns the total number of packets queued across all VNs under
  // logical_queue_id, or 0 if the logical queue has never been registered.
  size_t size(size_t logical_queue_id) const {
    LockType lock(mutex);
    auto it = logical_queues_info.find(logical_queue_id);
    if (it == logical_queues_info.end()) return 0;
    return it->second.size;
  }

  // Returns the number of packets queued for VN vn_queue_id under
  // logical_queue_id.
  size_t size(size_t logical_queue_id, size_t vn_queue_id) const {
    LockType lock(mutex);
    auto it = logical_queues_info.find(logical_queue_id);
    if (it == logical_queues_info.end()) return 0;
    return it->second.vn_queues[vn_queue_id].size;
  }

  // Set the maximum number of packets that VN vn_queue_id may hold under
  // logical_queue_id.  Packets arriving when the VN queue is full are dropped
  // immediately without blocking.
  void set_vn_capacity(size_t logical_queue_id, size_t vn_queue_id,
                          size_t c) {
    LockType lock(mutex);
    auto &w_info = workers_info[map_to_worker(logical_queue_id)];
    auto &lq = get_logical_queue(logical_queue_id, w_info);
    lq.vn_queues[vn_queue_id].capacity = c;
  }

  // Set the per-VN capacity for all VNs under logical_queue_id to c.
  void set_queue_capacity(size_t logical_queue_id, size_t c) {
    LockType lock(mutex);
    auto &w_info = workers_info[map_to_worker(logical_queue_id)];
    auto &lq = get_logical_queue(logical_queue_id, w_info);
    for (size_t i = 0; i < nb_vns; i++)
      lq.vn_queues[i].capacity = c;
  }

  // Set the per-VN capacity to c for every VN in every registered logical
  // queue, and update the default capacity used when new logical queues are
  // lazily initialised.
  void set_capacity_for_all(size_t c) {
    LockType lock(mutex);
    for (auto &p : logical_queues_info) {
      for (size_t i = 0; i < nb_vns; i++)
        p.second.vn_queues[i].capacity = c;
    }
    capacity = c;
  }

  // Set the dequeue rate limit for logical_queue_id to bps bits per second.
  // Pass 0 to disable rate limiting.  This affects only dequeue_rate_bps in
  // the leaky bucket; the burst ceiling tau is not modified.
  void set_rate(size_t logical_queue_id, uint64_t bps) {
    LockType lock(mutex);
    auto &w_info = workers_info[map_to_worker(logical_queue_id)];
    auto &lq = get_logical_queue(logical_queue_id, w_info);
    lq.dequeue_rate_bps = (double)bps;
  }

  // Set the dequeue rate limit to bps for all currently registered logical
  // queues.  Logical queues registered after this call will still be
  // initialised with default_dequeue_rate; call set_rate() individually after
  // registration if they should inherit this rate.
  void set_rate_for_all(uint64_t bps) {
    LockType lock(mutex);
    for (auto &p : logical_queues_info)
      p.second.dequeue_rate_bps = (double)bps;
  }

  //! Deleted copy constructor
  QueueingLogicVN(const QueueingLogicVN &) = delete;
  //! Deleted copy assignment operator
  QueueingLogicVN &operator =(const QueueingLogicVN &) = delete;

  //! Deleted move constructor
  QueueingLogicVN(QueueingLogicVN &&) = delete;
  //! Deleted move assignment operator
  QueueingLogicVN &&operator =(QueueingLogicVN &&) = delete;

 private:
  using ticks = std::chrono::nanoseconds;
  using clock = std::chrono::high_resolution_clock;

  // Queue element stored in each VN's physical deque.
  //
  // pkt_size is captured from the packet at enqueue time and stored in the
  // element so that the scheduler can read it during dequeue without
  // dereferencing the packet object.  This is important for the move-semantic
  // push_front overload, where the packet has already been moved into the deque
  // by the time the scheduler inspects the element.
  struct QE {
    QE(T e, size_t color, size_t pkt_size)
        : e(std::move(e)),
          color(color),
          pkt_size(pkt_size) {}

    T e;
    size_t color;     // 0 = Green (eligible for Phase 1 GB-credit service),
                       // 1 = Yellow (eligible for Phase 2 DWRR service only)
    size_t pkt_size;  // Packet size in bytes, fixed at enqueue time; used by
                       // the leaky bucket (T_bits) and by select_vn() to deduct
                       // green_credit and weighted_deficit without re-reading
                       // the packet body
  };

  // Per-VN scheduling state maintained within a logical queue.
  //
  // One VNQueueInfo exists for each virtual network i under a logical queue,
  // indexed by vn_queue_id in LogicalQueueInfo::vn_queues.
  struct VNQueueInfo {
    VNQueueInfo(size_t capacity, int weight)
        : capacity(capacity), weight(weight) {}

    size_t size{0};      // Current number of packets in this VN's deque;
                          // incremented by push_front, decremented by pop_back
    size_t capacity;     // Maximum number of packets; packets arriving when
                          // size >= capacity are dropped without blocking

    // GB credit counter gamma_i (bytes).  Incremented by pkt_size each time a
    // green packet is enqueued.  Decremented by the head-of-line packet size
    // each time Phase 1 selects this VN.  May go negative (surplus semantics):
    // a large head-of-line packet is allowed to overdraw the credit balance,
    // and the debt is repaid by subsequent green arrivals.
    int green_credit{0};

    // DWRR deficit counter for Phase 2 (bytes).  Incremented by weight * Q at
    // the start of each DWRR round in which no VN has positive deficit.
    // Decremented by the head-of-line packet size when this VN is selected.
    // May go negative; the balance carries over across rounds so that the
    // long-run byte throughput ratio converges to w_1 : w_2 : ... : w_k.
    // Reset to zero when the VN is removed from the active set by the
    // idle-timeout sweep.
    int weighted_deficit{0};

    int weight;  // Sharing weight w_i; set at construction from vn_weights[i]
                  // and not modified at runtime

    // Time at which this VN most recently transitioned from non-empty to empty.
    // Sentinel value clock::time_point::min() means the VN has never been empty.
    // Written in pop_back after a dequeue leaves the VN queue empty; read only
    // when size == 0 during the idle-timeout sweep, so the sentinel value
    // requires no special-case handling in the hot path.
    clock::time_point empty_since{clock::time_point::min()};
  };

  // Per-logical-queue scheduling and rate-limiting state.
  //
  // A logical queue typically corresponds to one egress port.  It owns one
  // VNQueueInfo per virtual network and one leaky bucket that enforces the
  // per-port dequeue rate limit.
  struct LogicalQueueInfo {
    LogicalQueueInfo(size_t capacity, const std::vector<int> &weights)
        : LCT(clock::now()),
          X(0.0),
          dequeue_rate_bps(default_dequeue_rate),
          tau(default_tau) {
      for (size_t i = 0; i < weights.size(); i++)
        vn_queues.push_back(VNQueueInfo(capacity, weights[i]));
      // Pre-allocate active_set to nb_vns entries so that push_back operations
      // in push_front never trigger a reallocation in the hot path.
      active_set.reserve(weights.size());
    }

    std::vector<VNQueueInfo> vn_queues;  // Indexed by vn_queue_id

    size_t size{0};           // Total packets across all VNs in this logical queue

    // Leaky-bucket state for the per-port dequeue rate limiter.
    clock::time_point LCT;    // Last time the water level X was updated
    double X;                 // Current virtual water level in bits
    double dequeue_rate_bps;  // Drain rate in bits/s; 0.0 disables rate limiting
    double tau;               // Burst ceiling in bits

    // VN id selected by select_vn() but not yet released by the leaky bucket.
    // -1 means no pending decision.  Caching the result prevents select_vn()
    // from being called a second time for the same packet when the leaky bucket
    // defers dequeue, which would advance GB credits and DWRR deficits twice
    // for a single transmission and could select a different VN on retry.
    int pending_vn{-1};

    // Ring-scan cursors for select_vn().  Both store indices into active_set
    // (not vn_queue_id values) so that a simple modulo operation positions the
    // cursor after an erase() shifts subsequent elements.  A minor positional
    // drift after erase is acceptable; it does not affect correctness.
    size_t green_scan_idx{0};  // Phase 1 (GB-credit) scan cursor
    size_t wrr_idx{0};         // Phase 2 (DWRR) scan cursor

    // Set of VNs currently competing for transmission, ordered by activation
    // time.  A VN is appended when it first receives a packet (or reactivates
    // after being removed); it is removed by the idle-timeout sweep in pop_back
    // after remaining continuously empty for longer than vn_idle_timeout.
    //
    // Members are not guaranteed to have size > 0: there is a window between a
    // VN becoming empty and the sweep removing it.  Both phases of select_vn()
    // and the sweep itself must explicitly skip members with size == 0.
    std::vector<size_t> active_set;
  };

  // Per-worker runtime state.
  //
  // map_to_worker(logical_queue_id) assigns each logical queue to a worker.
  // Workers operate independently; there is no cross-worker fairness guarantee.
  struct WorkerInfo {
    std::condition_variable q_not_empty{};
    size_t size{0};           // Total packets across all logical queues owned by
                               // this worker; pop_back blocks when this is zero
    size_t worker_rr_ptr{0};  // Round-robin scan cursor over logical_queue_ids

    // Ordered list of registered logical queue ids for O(n) sequential scanning
    // in pop_back without iterating over the unordered_map.
    std::vector<size_t> logical_queue_ids{};

    // Physical packet storage: logical_queue_id -> per-VN deques.
    // Each deque is strictly FIFO; pop_back always removes from the front.
    std::unordered_map<size_t, std::vector<std::deque<QE>>> physical_queues{};

    size_t wrapping_counter{0};  // Reserved; currently unused
  };

  // Select the VN to serve next from logical queue lq using the two-phase
  // CMCS scheduling algorithm.
  //
  // Precondition: lq.size > 0, so active_set is non-empty and at least one
  // member has a non-empty deque.  Both phases skip members with size == 0
  // because the idle-timeout sweep may not yet have removed them.
  //
  // Phase 1 – GB-credit priority service.
  // Scans active_set starting from green_scan_idx in round-robin order.
  // Selects the first VN with green_credit > 0 and a non-empty queue, deducts
  // the head-of-line packet size from green_credit (allowing overdraft), and
  // returns that VN id.  If no VN qualifies, falls through to Phase 2.
  //
  // Phase 2 – DWRR residual-capacity sharing.
  // Scans active_set starting from wrr_idx.  Selects the first non-empty VN
  // with weighted_deficit > 0, deducts the head-of-line size (allowing
  // overdraft), and returns that VN id.  If no VN qualifies in the current
  // scan, replenishes weighted_deficit += weight * Q for all active VNs and
  // retries.  Because the precondition guarantees at least one non-empty VN,
  // replenishment always produces at least one positive deficit and the loop
  // terminates.
  size_t select_vn(LogicalQueueInfo &lq,
                      std::vector<std::deque<QE>> &phys_queues) {

    // Phase 1: scan for a VN with positive GB credit.
    {
      size_t n = lq.active_set.size();
      size_t start = lq.green_scan_idx % n;

      for (size_t cnt = 0; cnt < n; cnt++) {
        size_t idx = (start + cnt) % n;
        size_t i   = lq.active_set[idx];

        auto &sq = lq.vn_queues[i];
        if (sq.size == 0) continue;
        if (sq.green_credit > 0) {
          size_t head_sz    = phys_queues[i].front().pkt_size;
          sq.green_credit  -= (int)head_sz;
          // Advance the cursor past the selected position so the next call
          // starts scanning from the following VN, distributing service
          // opportunities fairly across all VNs with positive GB credit.
          lq.green_scan_idx = idx + 1;
          return i;
        }
      }
    }

    // Phase 2: DWRR scan over all active VNs.
    // Per-packet round-robin: after serving one packet from the selected VN,
    // wrr_idx advances to the next candidate so that every non-empty VN with
    // positive deficit gets a turn before any VN is served twice.  The weight
    // ratio is expressed through weighted_deficit: a VN with a higher weight
    // accumulates deficit faster and therefore qualifies more frequently.
    while (true) {
      size_t n = lq.active_set.size();
      size_t start = lq.wrr_idx % n;

      for (size_t cnt = 0; cnt < n; cnt++) {
        size_t idx = (start + cnt) % n;
        size_t i   = lq.active_set[idx];

        auto &sq = lq.vn_queues[i];
        if (sq.size != 0 && sq.weighted_deficit > 0) {
          size_t head_sz        = phys_queues[i].front().pkt_size;
          sq.weighted_deficit  -= (int)head_sz;
          lq.wrr_idx            = idx + 1;
          return i;
        }
      }

      // No VN has a positive deficit: replenish all active VNs proportionally
      // to their sharing weights and retry.  Only VNs in the active set receive
      // replenishment, so long-idle VNs that have been removed cannot
      // accumulate stale deficit.
      for (size_t i : lq.active_set)
        lq.vn_queues[i].weighted_deficit += lq.vn_queues[i].weight * (int)Q;
    }
  }

  // Return a reference to the LogicalQueueInfo for logical_queue_id, creating
  // it lazily on first access.  Initialisation allocates nb_vns VNQueueInfo
  // entries using the current default capacity and vn_weights, and registers
  // the corresponding physical deques in w_info.  This is the only code path
  // that creates LogicalQueueInfo or physical_queues entries, ensuring that
  // both sides are always initialised together.
  LogicalQueueInfo &get_logical_queue(size_t logical_queue_id, WorkerInfo &w_info) {
    auto it = logical_queues_info.find(logical_queue_id);
    if (it != logical_queues_info.end()) return it->second;

    auto p = logical_queues_info.emplace(logical_queue_id,
                                          LogicalQueueInfo(capacity, vn_weights));

    w_info.physical_queues.emplace(logical_queue_id,
                                    std::vector<std::deque<QE>>(nb_vns));
    w_info.logical_queue_ids.push_back(logical_queue_id);

    return p.first->second;
  }

  mutable MutexType mutex;
  size_t nb_workers;
  size_t capacity;              // Default per-VN queue capacity; updated by
                                 // set_capacity_for_all and used when a new
                                 // logical queue is lazily initialised
  std::vector<int> vn_weights;  // Sharing weights w_1 ... w_k
  size_t nb_vns;                 // Number of virtual networks; equals vn_weights.size()

  // Default dequeue rate for newly created logical queues (bits/s).
  static constexpr double default_dequeue_rate  = 10.0 * 1000000.0;  // 10 Mbit/s

  // Default burst ceiling for the dequeue leaky bucket (bits).
  // Set to four maximum-sized Ethernet frames (4 x 1514 bytes x 8 bits).
  static constexpr double default_tau = 4.0 * 1514.0 * 8.0;

  // DWRR quantum Q (bytes): the amount of deficit added to each active VN per
  // replenishment round.  Sized to one maximum Ethernet frame (1514 bytes) so
  // that a single replenishment always provides enough credit to transmit at
  // least one maximum-sized packet.
  static constexpr size_t Q = 1514;

  // Duration after which an empty VN is considered idle and removed from the
  // active set.  Removal resets weighted_deficit to zero so that the VN cannot
  // monopolise the link when it reactivates after a long silence.
  static constexpr ticks vn_idle_timeout =
      std::chrono::duration_cast<ticks>(std::chrono::milliseconds(500));

  std::unordered_map<size_t, LogicalQueueInfo> logical_queues_info{};
  std::vector<WorkerInfo> workers_info{};
  FMap map_to_worker;
};

}  // namespace bm

#endif  // BM_BM_SIM_QUEUEING_H_
