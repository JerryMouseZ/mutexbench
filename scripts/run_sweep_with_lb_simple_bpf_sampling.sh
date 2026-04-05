#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SCHED_EXT_STATE="/sys/kernel/sched_ext/state"
SCHED_EXT_OPS="/sys/kernel/sched_ext/root/ops"

usage() {
  cat <<'EOF'
Run a sweep command and capture lb_simple BPF samples in parallel.

Usage:
  sudo scripts/run_sweep_with_lb_simple_bpf_sampling.sh [launcher-options] -- <sweep-command> [args...]

Launcher options:
  --interval-us N          Sampler interval in microseconds (default: 100)
  --sampler-output PATH    CSV path for sampler output (default: ./lb_simple_bpf_samples.csv)
  --sampler-log PATH       Log path for sampler stderr/stdout (default: <sampler-output>.log)
  --sampler-script PATH    Sampler script path (default: scripts/sample_lb_simple_bpf.py)
  --ops-name NAME          Expected sched_ext ops name (default: lb_simple)
  --wait-timeout-s N       Seconds to wait for lb_simple to attach (default: 30)
  --poll-interval-ms N     Poll interval while waiting for sched_ext (default: 20)
  --include-agg            Pass --include-agg to the sampler
  --include-stats-map      Pass --include-stats-map to the sampler
  --include-slots          Pass --include-slots to the sampler
  --slot-limit N           Pass --slot-limit to the sampler (default: 16)
  -h, --help               Show this help

Everything after -- is executed as the sweep command.

Example:
  sudo scripts/run_sweep_with_lb_simple_bpf_sampling.sh \
    --interval-us 100 \
    --sampler-output /tmp/lb_simple_bpf.csv \
    --include-agg \
    --include-slots \
    -- \
    bench/mutexbench/scripts/sweep_mutex_throughput.sh \
      --threads 32 --critical-ns 350 --outside-ns 350 \
      --duration-ms 3000 --warmup-duration-ms 1000 --repeats 3 \
      --timeslice-extension off \
      --bench-ld-preload target/release/liblb_simple.so \
      --lock-kind mutex \
      --output-raw /tmp/lb_raw.csv \
      --output-summary /tmp/lb_summary.csv
EOF
}

is_uint() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+$ ]]
}

ensure_parent_dir() {
  local path="$1"
  local parent=""

  parent="$(dirname -- "$path")"
  mkdir -p -- "$parent"
}

restore_owner_if_sudo_user() {
  local path="$1"
  local sudo_uid="${SUDO_UID:-}"
  local sudo_gid="${SUDO_GID:-}"

  if [[ -z "$sudo_uid" || -z "$sudo_gid" ]]; then
    return 0
  fi
  if [[ -e "$path" ]]; then
    chown "$sudo_uid:$sudo_gid" "$path" 2>/dev/null || true
  fi
}

read_file_trimmed() {
  local path="$1"
  if [[ -r "$path" ]]; then
    tr -d '\n' < "$path"
  fi
}

sched_ext_ops_matches() {
  local ops="$1"
  [[ "$ops" == "$ops_name" || "$ops" == "${ops_name}_"* ]]
}

sched_ext_is_ready() {
  local state=""
  local ops=""

  state="$(read_file_trimmed "$SCHED_EXT_STATE")"
  ops="$(read_file_trimmed "$SCHED_EXT_OPS")"
  [[ "$state" == "enabled" ]] && sched_ext_ops_matches "$ops"
}

cleanup() {
  local exit_status=$?

  trap - EXIT INT TERM

  if [[ -n "${sampler_pid:-}" ]] && kill -0 "$sampler_pid" >/dev/null 2>&1; then
    kill -TERM "$sampler_pid" >/dev/null 2>&1 || true
    wait "$sampler_pid" >/dev/null 2>&1 || true
  fi

  if [[ -n "${sweep_pid:-}" ]] && kill -0 "$sweep_pid" >/dev/null 2>&1; then
    kill -TERM "$sweep_pid" >/dev/null 2>&1 || true
    wait "$sweep_pid" >/dev/null 2>&1 || true
  fi

  restore_owner_if_sudo_user "$sampler_output"
  restore_owner_if_sudo_user "$sampler_log"

  exit "$exit_status"
}

interval_us=100
sampler_output="./lb_simple_bpf_samples.csv"
sampler_log=""
sampler_script="$SCRIPT_DIR/sample_lb_simple_bpf.py"
ops_name="lb_simple"
wait_timeout_s=30
poll_interval_ms=20
include_agg=0
include_stats_map=0
include_slots=0
slot_limit=16

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval-us)
      interval_us="${2:-}"
      shift 2
      ;;
    --sampler-output)
      sampler_output="${2:-}"
      shift 2
      ;;
    --sampler-log)
      sampler_log="${2:-}"
      shift 2
      ;;
    --sampler-script)
      sampler_script="${2:-}"
      shift 2
      ;;
    --ops-name)
      ops_name="${2:-}"
      shift 2
      ;;
    --wait-timeout-s)
      wait_timeout_s="${2:-}"
      shift 2
      ;;
    --poll-interval-ms)
      poll_interval_ms="${2:-}"
      shift 2
      ;;
    --include-agg)
      include_agg=1
      shift
      ;;
    --include-stats-map)
      include_stats_map=1
      shift
      ;;
    --include-slots)
      include_slots=1
      shift
      ;;
    --slot-limit)
      slot_limit="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      echo "Unknown launcher argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ $# -eq 0 ]]; then
  echo "Missing sweep command. Pass it after --" >&2
  usage >&2
  exit 1
fi

if [[ "$EUID" -ne 0 ]]; then
  echo "Run this launcher as root, e.g. sudo $0 ..." >&2
  exit 1
fi

if ! is_uint "$interval_us" || [[ "$interval_us" -le 0 ]]; then
  echo "--interval-us must be a positive integer" >&2
  exit 1
fi
if ! is_uint "$wait_timeout_s" || [[ "$wait_timeout_s" -le 0 ]]; then
  echo "--wait-timeout-s must be a positive integer" >&2
  exit 1
fi
if ! is_uint "$poll_interval_ms" || [[ "$poll_interval_ms" -le 0 ]]; then
  echo "--poll-interval-ms must be a positive integer" >&2
  exit 1
fi
if ! is_uint "$slot_limit" || [[ "$slot_limit" -le 0 ]]; then
  echo "--slot-limit must be a positive integer" >&2
  exit 1
fi
if [[ ! -f "$sampler_script" ]]; then
  echo "Sampler script not found: $sampler_script" >&2
  exit 1
fi

if [[ -z "$sampler_log" ]]; then
  sampler_log="${sampler_output}.log"
fi

ensure_parent_dir "$sampler_output"
ensure_parent_dir "$sampler_log"

sampler_cmd=(
  python3 "$sampler_script"
  --interval-us "$interval_us"
  --output "$sampler_output"
  --ops-name "$ops_name"
  --quiet
)
if [[ "$include_agg" -eq 1 ]]; then
  sampler_cmd+=( --include-agg )
fi
if [[ "$include_stats_map" -eq 1 ]]; then
  sampler_cmd+=( --include-stats-map )
fi
if [[ "$include_slots" -eq 1 ]]; then
  sampler_cmd+=( --include-slots )
fi
sampler_cmd+=( --slot-limit "$slot_limit" )

sweep_cmd=( "$@" )
poll_sleep="$(printf '%d.%03d' $((poll_interval_ms / 1000)) $((poll_interval_ms % 1000)))"

trap cleanup EXIT INT TERM

printf '[launcher] sampler_output=%s\n' "$sampler_output" >&2
printf '[launcher] sampler_log=%s\n' "$sampler_log" >&2
printf '[launcher] starting sweep command: %q' "${sweep_cmd[0]}" >&2
for ((i = 1; i < ${#sweep_cmd[@]}; ++i)); do
  printf ' %q' "${sweep_cmd[$i]}" >&2
done
printf '\n' >&2

"${sweep_cmd[@]}" &
sweep_pid=$!

ready=0
for ((i = 0; i < wait_timeout_s * 1000; i += poll_interval_ms)); do
  if ! kill -0 "$sweep_pid" >/dev/null 2>&1; then
    wait "$sweep_pid"
    echo "Sweep command exited before $ops_name became active; sampler was not started." >&2
    exit 1
  fi

  if sched_ext_is_ready; then
    ready=1
    break
  fi

  sleep "$poll_sleep"
done

if [[ "$ready" -ne 1 ]]; then
  echo "Timed out waiting for sched_ext ops=$ops_name to become active." >&2
  exit 1
fi

sleep 0.05
printf '[launcher] starting sampler: %q' "${sampler_cmd[0]}" >&2
for ((i = 1; i < ${#sampler_cmd[@]}; ++i)); do
  printf ' %q' "${sampler_cmd[$i]}" >&2
done
printf '\n' >&2

"${sampler_cmd[@]}" >"$sampler_log" 2>&1 &
sampler_pid=$!

set +e
wait "$sweep_pid"
sweep_status=$?
set -e

if [[ -n "${sampler_pid:-}" ]] && kill -0 "$sampler_pid" >/dev/null 2>&1; then
  kill -TERM "$sampler_pid" >/dev/null 2>&1 || true
  wait "$sampler_pid" >/dev/null 2>&1 || true
fi

restore_owner_if_sudo_user "$sampler_output"
restore_owner_if_sudo_user "$sampler_log"

trap - EXIT INT TERM
exit "$sweep_status"
