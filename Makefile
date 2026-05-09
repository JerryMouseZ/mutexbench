CXX ?= g++
CPPFLAGS ?=
CXXFLAGS ?= -O3 -std=c++20 -pthread
LDFLAGS ?=
LDLIBS ?= -ldl

TARGETS := mutex_bench curve_bench
DEPFILES := $(TARGETS:%=%.d)
DEPFLAGS = -MMD -MP -MF $@.d -MT $@

.PHONY: all clean test-monotonic-timing test-remove-outside-iters test-sweep-throughput-cpu test-sweep-multi-lock-direct test-sweep-multi-lock-profile-reports

all: $(TARGETS)

-include $(DEPFILES)

mutex_bench: mutex_bench.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(DEPFLAGS) $< -o $@ $(LDFLAGS) $(LDLIBS)

curve_bench: curve_bench.cpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(DEPFLAGS) $< -o $@ $(LDFLAGS)

clean:
	rm -f $(TARGETS) $(DEPFILES)

test-monotonic-timing:
	bash scripts/test_monotonic_timing.sh

test-remove-outside-iters: mutex_bench
	bash scripts/test_remove_outside_iters.sh

test-sweep-throughput-cpu:
	bash scripts/test_sweep_mutex_throughput_cpu.sh

test-sweep-multi-lock-direct:
	bash scripts/test_sweep_multi_lock_direct.sh

test-sweep-multi-lock-profile-reports:
	bash scripts/test_sweep_multi_lock_profile_reports.sh
