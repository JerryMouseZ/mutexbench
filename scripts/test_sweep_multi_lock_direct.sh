#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

fake_sweep="$tmpdir/fake_sweep.sh"
fake_direct_lib="$tmpdir/libmcs_tas_accordin_direct.so"

cat > "$fake_sweep" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$fake_sweep"
: > "$fake_direct_lib"

output="$(
  MUTEXBENCH_MULTI_LOCK_LOCK_FILE="$tmpdir/queue.lock" \
  MCS_TAS_ACCORDIN_DIRECT_LIB="$fake_direct_lib" \
  "$SCRIPT_DIR/sweep_mutex_throughput_multi_lock.sh" \
    --locks mcs_tas_accordin \
    --sweep-script "$fake_sweep" \
    --output-root "$tmpdir/results" \
    --sudo-mode none \
    --threads 1 \
    --critical-ns 1 \
    --outside-ns 1 \
    --duration-ms 1 \
    --repeats 1 \
    --dry-run \
    2>&1
)"

if [[ "$output" != *"--lock-kind mcs_tas_accordin_direct"* ]]; then
  echo "expected mcs_tas_accordin to use direct benchmark lock kind" >&2
  echo "$output" >&2
  exit 1
fi

if [[ "$output" == *"--bench-ld-preload"* ]]; then
  echo "expected mcs_tas_accordin direct path not to use pthread hook preload" >&2
  echo "$output" >&2
  exit 1
fi

if [[ "$output" != *"MCS_TAS_ACCORDIN_DIRECT_LIB=$fake_direct_lib"* ]]; then
  echo "expected direct library path to be forwarded to mutex_bench" >&2
  echo "$output" >&2
  exit 1
fi
