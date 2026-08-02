/* Copyright 2013-present Barefoot Networks, Inc.
 * Copyright 2022 VMware, Inc.
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
 * Antonin Bas
 *
 */

#include <bm/bm_sim/options_parse.h>
#include <bm/bm_sim/target_parser.h>
#include <bm/bm_grpc/pem.h>

#include <exception>
#include <climits>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <streambuf>
#include <string>

#include "switch_runner.h"

int
main(int argc, char* argv[]) {
  bm::TargetParserBasicWithDynModules simple_switch_parser;
  simple_switch_parser.add_flag_option(
      "disable-swap",
      "Disable JSON swapping at runtime; this is not recommended when using "
      "P4Runtime!");
  simple_switch_parser.add_string_option(
      "grpc-server-addr",
      "Bind gRPC server to given address [default is 0.0.0.0:9559]");
  simple_switch_parser.add_flag_option(
      "grpc-server-ssl",
      "Enable SSL/TLS for gRPC server");
  simple_switch_parser.add_string_option(
      "grpc-server-cacert",
      "Path to pem file holding CA certificate to verify peer against");
  simple_switch_parser.add_string_option(
      "grpc-server-cert",
      "Path to pem file holding server certificate");
  simple_switch_parser.add_string_option(
      "grpc-server-key",
      "Path to pem file holding server key");
  simple_switch_parser.add_flag_option(
      "grpc-server-with-client-auth",
      "Require client to have a valid certificate for mutual authentication");
  simple_switch_parser.add_uint_option(
      "cpu-port",
      "Choose a numerical value for the CPU port, it will be used for "
      "packet-in / packet-out. Do not add an interface with this port number, "
      "and 0 is not a valid value. "
      "When using standard v1model.p4, this value must fit within 9 bits. "
      "If you do not use this command-line option, "
      "P4Runtime packet IO functionality will not be available: you will not "
      "be able to receive / send packets using the P4Runtime StreamChannel "
      "bi-directional stream.");
  simple_switch_parser.add_uint_option(
      "drop-port",
      "Choose a numerical value for the drop port (default is 511). "
      "When using standard v1model.p4, this value must fit within 9 bits. "
      "You will need to use this command-line option when you wish to use port "
      "511 as a valid dataplane port or as the CPU port.");
  simple_switch_parser.add_string_option(
      "dp-grpc-server-addr",
      "Use a gRPC channel to inject and receive dataplane packets; "
      "bind this gRPC server to given address, e.g. 0.0.0.0:50052");
  // CMCS modification: replace --priority-queues <uint> with --vn-weights <string>.
  //
  // The original --priority-queues option accepted a single integer specifying
  // only the number of priority queues.  This has been replaced by --vn-weights,
  // which accepts a comma-separated list of positive integers representing the
  // Deficit Weighted Round Robin (DWRR) sharing weights w_1, w_2, ..., w_k for
  // each virtual network (VN).  The number of VNs k is inferred directly from
  // the number of weights supplied, so no separate count parameter is needed.
  // When the option is omitted, the scheduler defaults to a single VN with
  // weight 1, preserving backward-compatible behaviour.
  simple_switch_parser.add_string_option(
      "vn-weights",
      "Comma-separated DWRR sharing weights for each virtual network (VN), "
      "e.g. \"1,2,5,10\". The number of VNs is inferred from the number of "
      "weights. If not provided, defaults to a single VN with weight 1.");
  simple_switch_parser.add_uint_option(
      "max-mc-groups",
      "Maximum number of multicast groups (default is "
      + std::to_string(
          sswitch_grpc::SimpleSwitchGrpcRunner::default_mgid_table_size)
      + ")");
  simple_switch_parser.add_uint_option(
      "max-l1-entries",
      "Maximum number of L1 multicast entries (default is "
      + std::to_string(
          sswitch_grpc::SimpleSwitchGrpcRunner::default_l1_max_entries)
      + ")");
  simple_switch_parser.add_uint_option(
      "max-l2-entries",
      "Maximum number of L2 multicast entries (default is "
      + std::to_string(
          sswitch_grpc::SimpleSwitchGrpcRunner::default_l2_max_entries)
      + ")");

  bm::OptionsParser parser;
  parser.parse(argc, argv, &simple_switch_parser);

  std::string dp_grpc_server_addr;
  {
    auto rc = simple_switch_parser.get_string_option(
        "dp-grpc-server-addr", &dp_grpc_server_addr);
    if (rc != bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED &&
        rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  bool disable_swap_flag = false;
  {
    auto rc = simple_switch_parser.get_flag_option(
        "disable-swap", &disable_swap_flag);
    if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS) std::exit(1);
  }

  std::string grpc_server_addr;
  {
    auto rc = simple_switch_parser.get_string_option(
        "grpc-server-addr", &grpc_server_addr);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      grpc_server_addr = "0.0.0.0:9559";
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  bool grpc_server_ssl = false;
  {
    auto rc = simple_switch_parser.get_flag_option(
        "grpc-server-ssl", &grpc_server_ssl);
    if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS) std::exit(1);
  }

  std::string grpc_server_cacert;
  {
    auto rc = simple_switch_parser.get_string_option(
        "grpc-server-cacert", &grpc_server_cacert);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      grpc_server_cacert = "";
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  std::string grpc_server_cert;
  {
    auto rc = simple_switch_parser.get_string_option(
        "grpc-server-cert", &grpc_server_cert);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      grpc_server_cert = "";
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  std::string grpc_server_key;
  {
    auto rc = simple_switch_parser.get_string_option(
        "grpc-server-key", &grpc_server_key);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      grpc_server_key = "";
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  bool grpc_server_with_client_auth = false;
  {
    auto rc = simple_switch_parser.get_flag_option(
        "grpc-server-with-client-auth", &grpc_server_with_client_auth);
    if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS) std::exit(1);
  }

  if (!grpc_server_ssl &&
      (grpc_server_cacert != "" ||
       grpc_server_cert != "" ||
       grpc_server_key != "")) {
    std::cerr << "SSL/TLS is disabled for gRPC server, "
        << "so provided .pem files will be ignored\n";
  }

  if (!grpc_server_ssl && grpc_server_with_client_auth) {
    std::cerr << "SSL/TLS is disabled for gRPC server, "
        << "so cannot request client auth\n";
  }

  if (grpc_server_ssl && grpc_server_cert == "") {
    std::cerr << "When enabling SSL/TLS for gRPC server, "
        << "--grpc-server-cert is required\n";
    std::exit(1);
  }
  if (grpc_server_ssl && grpc_server_key == "") {
    std::cerr << "When enabling SSL/TLS for gRPC server, "
        << "--grpc-server-key is required\n";
    std::exit(1);
  }

  uint32_t cpu_port = 0xffffffff;
  {
    auto rc = simple_switch_parser.get_uint_option("cpu-port", &cpu_port);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      cpu_port = 0;
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS || cpu_port == 0)
      std::exit(1);
  }

  uint32_t drop_port = 0xffffffff;
  {
    auto rc = simple_switch_parser.get_uint_option("drop-port", &drop_port);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      drop_port = sswitch_grpc::SimpleSwitchGrpcRunner::default_drop_port;
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
  }

  auto ssl_options = std::make_shared<sswitch_grpc::SSLOptions>();
  try {
    if (grpc_server_ssl) {
      if (grpc_server_cacert != "") {
        ssl_options->pem_root_certs = bm::read_pem_file(grpc_server_cacert);
      }
      if (grpc_server_cert != "") {
        ssl_options->pem_cert_chain = bm::read_pem_file(grpc_server_cert);
      }
      if (grpc_server_key != "") {
        ssl_options->pem_private_key = bm::read_pem_file(grpc_server_key);
      }
      ssl_options->with_client_auth = grpc_server_with_client_auth;
    }
  } catch (const bm::read_pem_exception &e) {
    std::cerr << e.msg();
    std::exit(1);
  }

  // Parse --vn-weights into a std::vector<int> that will be forwarded to
  // QueueingLogicVN as the DWRR sharing-weight array w_1 ... w_k.
  //
  // Parsing steps:
  //   1. Read the raw comma-separated string (e.g. "1,2,5,10").
  //   2. Split on ',' and strip leading/trailing whitespace from each token
  //      to tolerate user input such as "1, 2, 5, 10".
  //   3. Convert each token to int via std::stoi; catch conversion exceptions
  //      to give a clear error message on malformed input.
  //   4. Reject any weight that is not strictly positive, as the DWRR
  //      scheduler requires w_i > 0 for all virtual networks.
  //   5. If the option is absent, fall back to {1} (one VN, weight = 1).
  //
  // The number of VNs k is not passed separately; it is inferred from
  // vn_weights.size() inside QueueingLogicVN.
  std::vector<int> vn_weights;
  {
    std::string weights_str;
    auto rc = simple_switch_parser.get_string_option(
        "vn-weights", &weights_str);

    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED) {
      // Default: single VN with sharing weight 1.
      vn_weights = {1};
    } else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS) {
      std::exit(1);
    } else {
      // Tokenise the comma-separated weight string.
      std::istringstream ss(weights_str);
      std::string token;
      while (std::getline(ss, token, ',')) {
        // Strip leading and trailing whitespace.
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (token.empty()) continue;

        try {
          int w = std::stoi(token);
          if (w <= 0) {
            std::cerr << "--vn-weights: all weights must be > 0\n";
            std::exit(1);
          }
          vn_weights.push_back(w);
        } catch (const std::exception &) {
          // std::stoi throws std::invalid_argument or std::out_of_range on
          // non-numeric or overflow input.
          std::cerr << "--vn-weights: invalid token \"" << token << "\"\n";
          std::exit(1);
        }
      }
      if (vn_weights.empty()) {
        std::cerr << "--vn-weights: no valid weights parsed\n";
        std::exit(1);
      }
    }
  }

  uint32_t mgid_table_size = 0xffffffff;
  {
    auto rc = simple_switch_parser.get_uint_option(
        "max-mc-groups", &mgid_table_size);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      mgid_table_size =
          sswitch_grpc::SimpleSwitchGrpcRunner::default_mgid_table_size;
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
    if (mgid_table_size == 0 || mgid_table_size > INT_MAX) {
      std::cerr << "max-mc-groups must be between 1 and "
                << INT_MAX << std::endl;
      std::exit(1);
    }
  }

  uint32_t l1_max_entries = 0xffffffff;
  {
    auto rc = simple_switch_parser.get_uint_option(
        "max-l1-entries", &l1_max_entries);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      l1_max_entries =
          sswitch_grpc::SimpleSwitchGrpcRunner::default_l1_max_entries;
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
    if (l1_max_entries == 0 || l1_max_entries > INT_MAX) {
      std::cerr << "max-l1-entries must be between 1 and "
                << INT_MAX << std::endl;
      std::exit(1);
    }
  }

  uint32_t l2_max_entries = 0xffffffff;
  {
    auto rc = simple_switch_parser.get_uint_option(
        "max-l2-entries", &l2_max_entries);
    if (rc == bm::TargetParserBasic::ReturnCode::OPTION_NOT_PROVIDED)
      l2_max_entries =
          sswitch_grpc::SimpleSwitchGrpcRunner::default_l2_max_entries;
    else if (rc != bm::TargetParserBasic::ReturnCode::SUCCESS)
      std::exit(1);
    if (l2_max_entries == 0 || l2_max_entries > INT_MAX) {
      std::cerr << "max-l2-entries must be between 1 and "
                << INT_MAX << std::endl;
      std::exit(1);
    }
  }

  // CMCS modification: the seventh argument to get_instance() has changed
  // from priority_queues (uint32_t) to vn_weights (std::vector<int>).
  // SimpleSwitchGrpcRunner::get_instance() forwards this vector to
  // SimpleSwitch, which in turn passes it to QueueingLogicVN so that the
  // DWRR second-phase scheduler is initialised with the correct per-VN
  // sharing weights w_1 ... w_k.
  auto &runner = sswitch_grpc::SimpleSwitchGrpcRunner::get_instance(
      !disable_swap_flag,
      grpc_server_addr,
      cpu_port,
      dp_grpc_server_addr,
      drop_port,
      grpc_server_ssl ? ssl_options : nullptr,
      vn_weights,
      mgid_table_size, l1_max_entries, l2_max_entries);
  int status = runner.init_and_start(parser);
  if (status != 0) std::exit(status);

  runner.wait();
  return 0;
}