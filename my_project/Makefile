# SPDX-FileCopyrightText: 2018 Nate Foster
#
# SPDX-License-Identifier: Apache-2.0

BMV2_SWITCH_EXE = simple_switch_grpc
TOPO = topo/topology.json

include ./utils/Makefile

# Run the automated multi-VN throughput experiment.
# Reuses all build and run parameters from the base Makefile and appends
# --test to instruct run_exercise.py to invoke run_test.py after the
# Mininet topology and P4 switch have been initialized.
test: build
	sudo PATH=$(PATH) ${P4_EXTRA_SUDO_OPTS} python3 $(RUN_SCRIPT) -t $(TOPO) $(run_args) --test