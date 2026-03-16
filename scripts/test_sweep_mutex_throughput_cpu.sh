#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf -- "$tmpdir"
}
trap cleanup EXIT

fake_bin_dir="$tmpdir/bin"
mkdir -p "$fake_bin_dir"

cat <<'EOF' >"$fake_bin_dir/fake_mutex_bench"
#!/usr/bin/env bash
set -euo pipefail

sleep 0.2
cat <<'OUT'
throughput_ops_per_sec: 123.000000
elapsed_seconds: 1.000000
total_operations: 123.000000
avg_lock_hold_ns: 10.000000
avg_wait_ns_estimated: 20.000000
avg_lock_handoff_ns_estimated: 30.000000
lock_hold_samples: 40
OUT
EOF
chmod +x "$fake_bin_dir/fake_mutex_bench"

cat <<'EOF' >"$fake_bin_dir/pidstat"
#!/usr/bin/env bash
set -euo pipefail

pid=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p)
      pid="${2:?}"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [[ -z "$pid" ]]; then
  echo "missing -p PID" >&2
  exit 1
fi

trap 'exit 0' TERM INT

cat <<OUT
Linux 6.8.0-test (fake-host)  03/16/2026  _x86_64_  (8 CPU)

12:00:00      UID       PID    %usr %system  %guest   %wait    %CPU   CPU  Command
12:00:01     1000      ${pid}   10.00    0.00    0.00    0.00   10.00     0  mutex_bench
12:00:02     1000      ${pid}   40.00    0.00    0.00    0.00   40.00     0  mutex_bench
12:00:03     1000      ${pid}   60.00    0.00    0.00    0.00   60.00     0  mutex_bench
OUT

while kill -0 "$pid" >/dev/null 2>&1; do
  sleep 0.05
done
EOF
chmod +x "$fake_bin_dir/pidstat"

PATH="$fake_bin_dir:$PATH" \
  bash "$MUTEXBENCH_DIR/scripts/sweep_mutex_throughput.sh" \
    --binary "$fake_bin_dir/fake_mutex_bench" \
    --threads 2 \
    --critical-ns 3 \
    --outside-ns 5 \
    --duration-ms 1000 \
    --warmup-duration-ms 0 \
    --repeats 1 \
    --output-raw "$tmpdir/raw.csv" \
    --output-summary "$tmpdir/summary.csv"

PYTHONPATH="$MUTEXBENCH_DIR" python3 - "$tmpdir/raw.csv" "$tmpdir/summary.csv" <<'PY'
import csv
import sys
from pathlib import Path

import scripts.bench_csv_schema as bench_csv_schema

raw_path = Path(sys.argv[1])
summary_path = Path(sys.argv[2])

with raw_path.open(newline="") as handle:
    raw_reader = csv.DictReader(handle)
    raw_rows = list(raw_reader)

with summary_path.open(newline="") as handle:
    summary_reader = csv.DictReader(handle)
    summary_rows = list(summary_reader)

assert raw_reader.fieldnames is not None
assert summary_reader.fieldnames is not None
assert "avg_cpu_pct" in raw_reader.fieldnames, raw_reader.fieldnames
assert "avg_cpu_pct" in summary_reader.fieldnames, summary_reader.fieldnames
assert len(raw_rows) == 1, raw_rows
assert len(summary_rows) == 1, summary_rows
assert abs(float(raw_rows[0]["avg_cpu_pct"]) - 50.0) < 1e-9, raw_rows[0]
assert abs(float(summary_rows[0]["avg_cpu_pct"]) - 50.0) < 1e-9, summary_rows[0]
assert "avg_cpu_pct" in bench_csv_schema.RAW_FIELDNAMES
assert "avg_cpu_pct" in bench_csv_schema.SUMMARY_FIELDNAMES
PY
