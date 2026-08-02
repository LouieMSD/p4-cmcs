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

#ifndef SIMPLE_SWITCH_GRPC_SWITCH_RUNNER_H_
#define SIMPLE_SWITCH_GRPC_SWITCH_RUNNER_H_

#include <bm/bm_sim/dev_mgr.h>
#include <bm/bm_sim/simple_pre.h>

#include <grpcpp/server.h>

#include <memory>
#include <string>
// Required to pass virtual network (VN) sharing weights as a dynamically-sized
// vector from the command-line parser down to the traffic manager.
#include <vector>

class SimpleSwitch;

namespace bm {

class OptionsParser;

}  // namespace bm

namespace sswitch_grpc {

class SysrepoDriver;

class DataplaneInterfaceServiceImpl;

struct SSLOptions {
  std::string pem_root_certs;
  std::string pem_private_key;
  std::string pem_cert_chain;
  bool with_client_auth;
};

class SimpleSwitchGrpcRunner {
 public:
  static constexpr bm::DevMgrIface::port_t default_drop_port = 511;

  // The number of virtual networks and their respective sharing weights are
  // determined entirely by the vn_weights vector passed at construction time.
  // A single-element vector {1} is used as the default, which degrades the
  // scheduler to a simple FIFO with no inter-VN arbitration.
  static constexpr int default_mgid_table_size =
      bm::McSimplePre::DEFAULT_MGID_TABLE_SIZE;
  static constexpr int default_l1_max_entries =
      bm::McSimplePre::DEFAULT_L1_MAX_ENTRIES;
  static constexpr int default_l2_max_entries =
      bm::McSimplePre::DEFAULT_L2_MAX_ENTRIES;

  // there is no real need for a singleton here, except for the fact that we use
  // PIGrpcServerRunAddr, ... which uses static state
  static SimpleSwitchGrpcRunner &get_instance(
      bool enable_swap = false,
      std::string grpc_server_addr = "0.0.0.0:9559",
      bm::DevMgrIface::port_t cpu_port = 0,
      std::string dp_grpc_server_addr = "",
      bm::DevMgrIface::port_t drop_port = default_drop_port,
      std::shared_ptr<SSLOptions> ssl_options = nullptr,

      // Each element vn_weights[i] specifies the sharing weight w_i of VN i,
      // which governs how residual link capacity is distributed among VNs that
      // exceed their guaranteed bandwidth. The vector length implicitly defines
      // the total number of VNs managed by the Color-Aware Scheduler.
      // Defaults to {1} (one VN, weight 1) when --slice-weights is not provided.
      std::vector<int> vn_weights = {1},

      int mgid_table_size = default_mgid_table_size,
      int l1_max_entries = default_l1_max_entries,
      int l2_max_entries = default_l2_max_entries) {
    static SimpleSwitchGrpcRunner instance(
        enable_swap, grpc_server_addr, cpu_port, dp_grpc_server_addr,

        // Forward the full weights vector to the private constructor, which
        // passes it on to SimpleSwitch and ultimately to QueueingLogicVN.
        drop_port, ssl_options, vn_weights,

        mgid_table_size, l1_max_entries, l2_max_entries);
    return instance;
  }

  int init_and_start(const bm::OptionsParser &parser);
  void wait();
  void shutdown();
  int get_dp_grpc_server_port() {
    return dp_grpc_server_port;
  }
  void block_until_all_packets_processed();
  bool is_dp_service_active();

 private:
  // The private constructor accepts vn_weights and forwards it directly to the
  // SimpleSwitch constructor, which passes it further to the Traffic Manager.
  // This chain ensures that the per-VN FIFO queues and the Hybrid RR-DWRR
  // Scheduler are initialized with the correct number of VNs and their weights
  // before any packet arrives.
  SimpleSwitchGrpcRunner(bool enable_swap = false,
                         std::string grpc_server_addr = "0.0.0.0:9559",
                         bm::DevMgrIface::port_t cpu_port = 0,
                         std::string dp_grpc_server_addr = "",
                         bm::DevMgrIface::port_t drop_port = default_drop_port,
                         std::shared_ptr<SSLOptions> ssl_options = nullptr,
                         std::vector<int> vn_weights = {1},
                         int mgid_table_size = default_mgid_table_size,
                         int l1_max_entries = default_l1_max_entries,
                         int l2_max_entries = default_l2_max_entries);
  ~SimpleSwitchGrpcRunner();

  void port_status_cb(bm::DevMgrIface::port_t port,
                      const bm::DevMgrIface::PortStatus port_status);

  std::unique_ptr<SimpleSwitch> simple_switch;
  std::string grpc_server_addr;
  bm::DevMgrIface::port_t cpu_port;
  std::string dp_grpc_server_addr;
  int dp_grpc_server_port;
  DataplaneInterfaceServiceImpl *dp_service;
  std::unique_ptr<grpc::Server> dp_grpc_server;
#ifdef WITH_SYSREPO
  std::unique_ptr<SysrepoDriver> sysrepo_driver;
#endif  // WITH_SYSREPO
  std::shared_ptr<SSLOptions> ssl_options;
};

}  // namespace sswitch_grpc

#endif  // SIMPLE_SWITCH_GRPC_SWITCH_RUNNER_H_