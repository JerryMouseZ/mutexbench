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

assert_contains() {
  local needle="$1"
  local path="$2"
  if ! grep -Fq -- "$needle" "$path"; then
    echo "expected to find '$needle' in $path" >&2
    cat "$path" >&2
    exit 1
  fi
}

assert_not_contains() {
  local needle="$1"
  local path="$2"
  if grep -Fq -- "$needle" "$path"; then
    echo "did not expect to find '$needle' in $path" >&2
    cat "$path" >&2
    exit 1
  fi
}

expect_success() {
  local name="$1"
  shift
  if ! "$@" >"$tmpdir/$name.stdout" 2>"$tmpdir/$name.stderr"; then
    echo "expected success for $name" >&2
    cat "$tmpdir/$name.stderr" >&2
    exit 1
  fi
}

expect_failure() {
  local name="$1"
  shift
  if "$@" >"$tmpdir/$name.stdout" 2>"$tmpdir/$name.stderr"; then
    echo "expected failure for $name" >&2
    cat "$tmpdir/$name.stdout" >&2
    cat "$tmpdir/$name.stderr" >&2
    exit 1
  fi
}

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
OUT

while kill -0 "$pid" >/dev/null 2>&1; do
  sleep 0.01
done
EOF
chmod +x "$fake_bin_dir/pidstat"

make -C "$MUTEXBENCH_DIR" mutex_bench >/dev/null

expect_success \
  mutex_bench_outside_ns \
  "$MUTEXBENCH_DIR/mutex_bench" \
  --threads 1 \
  --duration-ms 1 \
  --warmup-duration-ms 0 \
  --critical-ns 0 \
  --outside-ns 1

expect_failure \
  mutex_bench_outside_iters \
  "$MUTEXBENCH_DIR/mutex_bench" \
  --threads 1 \
  --duration-ms 1 \
  --warmup-duration-ms 0 \
  --critical-ns 0 \
  --outside-iters 1
assert_contains "Unknown argument: --outside-iters" "$tmpdir/mutex_bench_outside_iters.stderr"

expect_success \
  sweep_outside_ns \
  env PATH="$fake_bin_dir:$PATH" bash "$MUTEXBENCH_DIR/scripts/sweep_mutex_throughput.sh" \
  --threads 1 \
  --critical-ns 0 \
  --outside-ns 1 \
  --duration-ms 1100 \
  --warmup-duration-ms 0 \
  --repeats 1 \
  --output-raw "$tmpdir/raw.csv" \
  --output-summary "$tmpdir/summary.csv"

expect_failure \
  sweep_outside_iters \
  bash "$MUTEXBENCH_DIR/scripts/sweep_mutex_throughput.sh" \
  --threads 1 \
  --critical-ns 0 \
  --outside-iters 1 \
  --duration-ms 1 \
  --warmup-duration-ms 0 \
  --repeats 1 \
  --output-raw "$tmpdir/raw-old.csv" \
  --output-summary "$tmpdir/summary-old.csv"
assert_contains "Unknown argument: --outside-iters" "$tmpdir/sweep_outside_iters.stderr"

python3 "$MUTEXBENCH_DIR/scripts/calibrate_iters.py" --help >"$tmpdir/calibrate_help.txt"
assert_contains "--outside-ns" "$tmpdir/calibrate_help.txt"
assert_not_contains "--outside-iters" "$tmpdir/calibrate_help.txt"

python3 "$MUTEXBENCH_DIR/scripts/recommend_threads.py" --help >"$tmpdir/recommend_help.txt"
assert_contains "--outside-ns" "$tmpdir/recommend_help.txt"
assert_not_contains "--outside-iters" "$tmpdir/recommend_help.txt"

assert_not_contains "--outside-iters" "$MUTEXBENCH_DIR/README.md"
