CXX ?= g++
CXXFLAGS ?= -O2 -std=c++20 -pthread
LDFLAGS ?=

TARGETS := mutex_bench curve_bench

.PHONY: all clean

all: $(TARGETS)

mutex_bench: mutex_bench.cpp
	$(CXX) $(CXXFLAGS) mutex_bench.cpp -o mutex_bench $(LDFLAGS)

curve_bench: curve_bench.cpp
	$(CXX) $(CXXFLAGS) curve_bench.cpp -o curve_bench $(LDFLAGS)

clean:
	rm -f $(TARGETS)
