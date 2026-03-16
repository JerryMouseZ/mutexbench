CXX ?= g++
CXXFLAGS ?= -O2 -std=c++20 -pthread
LDFLAGS ?=

TARGETS := mutex_bench curve_bench

.PHONY: all clean test-remove-outside-iters test-sweep-throughput-cpu

all: $(TARGETS)

mutex_bench: mutex_bench.cpp
	$(CXX) $(CXXFLAGS) mutex_bench.cpp -o mutex_bench $(LDFLAGS)

curve_bench: curve_bench.cpp
	$(CXX) $(CXXFLAGS) curve_bench.cpp -o curve_bench $(LDFLAGS)

clean:
	rm -f $(TARGETS)

test-remove-outside-iters: mutex_bench
	bash scripts/test_remove_outside_iters.sh

test-sweep-throughput-cpu:
	bash scripts/test_sweep_mutex_throughput_cpu.sh
