#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

make -C "$MUTEXBENCH_DIR" mutex_bench >/dev/null

output="$(
  "$MUTEXBENCH_DIR/mutex_bench" \
    --workload two-lock \
    --threads 4 \
    --duration-ms 100 \
    --warmup-duration-ms 0 \
    --group-a-critical-ns 3000 \
    --group-a-outside-ns 300 \
    --group-b-critical-ns 100 \
    --group-b-outside-ns 3000 \
    --lock-kind mutex
)"

require_metric() {
  local key="$1"
  if ! grep -q "^${key}: " <<< "$output"; then
    echo "missing metric: ${key}" >&2
    echo "$output" >&2
    exit 1
  fi
}

require_metric "workload"
require_metric "group_a_threads"
require_metric "group_b_threads"
require_metric "group_a_critical_ns"
require_metric "group_b_critical_ns"
require_metric "group_a_throughput_ops_per_sec"
require_metric "group_b_throughput_ops_per_sec"
require_metric "group_a_avg_lock_hold_ns"
require_metric "group_b_avg_lock_hold_ns"
require_metric "group_a_avg_lock_handoff_ns_estimated"
require_metric "group_b_avg_lock_handoff_ns_estimated"
require_metric "fairness_jain"
require_metric "group_a_normalized_slowdown"
require_metric "group_b_normalized_slowdown"

if ! grep -q "^workload: two-lock$" <<< "$output"; then
  echo "two-lock workload did not identify itself" >&2
  echo "$output" >&2
  exit 1
fi

if ! grep -q "^group_a_threads: 2$" <<< "$output"; then
  echo "group A should receive half of four threads" >&2
  echo "$output" >&2
  exit 1
fi

if ! grep -q "^group_b_threads: 2$" <<< "$output"; then
  echo "group B should receive half of four threads" >&2
  echo "$output" >&2
  exit 1
fi
