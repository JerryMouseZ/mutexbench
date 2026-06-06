#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Sweep multilockbench across lock kinds and Zipfian multi-lock parameters.

Usage:
  scripts/sweep_multilockbench.sh [options]

Options:
  --binary PATH                Benchmark binary path (default: <mutexbench>/multilockbench)
  --calibration-config PATH    Pass an explicit iter calibration config to multilockbench
  --lock-kinds CSV             Lock kinds to sweep (default: mutex)
  --lock-kind K                Alias for --lock-kinds K
  --threads CSV                Thread counts, comma-separated (default: 1,2,4,8,16,32,64)
  --lock-counts CSV            Independent lock counts, comma-separated (default: 4,16,64)
  --zipf-alpha CSV             Zipf skew values, comma-separated (default: 0,1.2,2.0)
  --critical-ns CSV            Critical-section burn time in ns (default: 100)
  --critical-iters CSV         Legacy alias for --critical-ns
  --outside-ns CSV             Non-critical-section burn time in ns (default: 1000)
  --duration-ms N              Measurement duration in ms (default: 2000)
  --warmup-duration-ms N       Warmup duration in ms (default: 50)
  --timing-sample-stride N     Timing sample stride (default: 8)
  --timeslice-extension M      off|auto|require (default: off)
  --seed N                     Base RNG seed (default: 1)
  --repeats N                  Runs per parameter point (default: 3)
  --output-root DIR            Output root for default raw/summary
  --output-raw PATH            Raw per-run CSV (default: <output-root>/raw.csv)
  --output-summary PATH        Aggregated CSV (default: <output-root>/summary.csv)
  -h, --help                   Show this help

Example:
  scripts/sweep_multilockbench.sh \
    --lock-kinds mutex,mcs-tas \
    --threads 16,32,64 \
    --lock-counts 16,64 \
    --zipf-alpha 0,1.2,2.0 \
    --critical-ns 300 \
    --outside-ns 3000 \
    --duration-ms 5000 \
    --repeats 3 \
    --output-root results/multilockbench
EOF
}

binary="$MUTEXBENCH_DIR/multilockbench"
calibration_config=""
lock_kinds_csv="mutex"
threads_csv="1,2,4,8,16,32,64"
lock_counts_csv="4,16,64"
zipf_alpha_csv="0,1.2,2.0"
critical_iters_csv="100"
outside_iters_csv="1000"
duration_ms="2000"
warmup_duration_ms="50"
timing_sample_stride="8"
timeslice_extension="off"
seed="1"
repeats="3"
output_root="$MUTEXBENCH_DIR/multilockbench_sweep"
output_raw=""
output_summary=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --calibration-config)
      calibration_config="${2:-}"
      shift 2
      ;;
    --lock-kinds|--lock-kind)
      lock_kinds_csv="${2:-}"
      shift 2
      ;;
    --threads)
      threads_csv="${2:-}"
      shift 2
      ;;
    --lock-counts|--num-locks)
      lock_counts_csv="${2:-}"
      shift 2
      ;;
    --zipf-alpha)
      zipf_alpha_csv="${2:-}"
      shift 2
      ;;
    --critical-ns|--critical-iters)
      critical_iters_csv="${2:-}"
      shift 2
      ;;
    --outside-ns)
      outside_iters_csv="${2:-}"
      shift 2
      ;;
    --duration-ms)
      duration_ms="${2:-}"
      shift 2
      ;;
    --warmup-duration-ms)
      warmup_duration_ms="${2:-}"
      shift 2
      ;;
    --timing-sample-stride)
      timing_sample_stride="${2:-}"
      shift 2
      ;;
    --timeslice-extension)
      timeslice_extension="${2:-}"
      shift 2
      ;;
    --seed)
      seed="${2:-}"
      shift 2
      ;;
    --repeats)
      repeats="${2:-}"
      shift 2
      ;;
    --output-root)
      output_root="${2:-}"
      shift 2
      ;;
    --output-raw)
      output_raw="${2:-}"
      shift 2
      ;;
    --output-summary)
      output_summary="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

is_uint() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+$ ]]
}

is_float() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

expand_home() {
  local path="$1"
  case "$path" in
    "~")
      printf "%s\n" "$HOME"
      ;;
    "~/"*)
      printf "%s/%s\n" "$HOME" "${path#~/}"
      ;;
    *)
      printf "%s\n" "$path"
      ;;
  esac
}

resolve_path() {
  local path="$1"
  local base_dir="$2"

  path="$(expand_home "$path")"
  case "$path" in
    /*)
      printf "%s\n" "$path"
      ;;
    *)
      printf "%s\n" "$base_dir/$path"
      ;;
  esac
}

resolve_executable_path() {
  local path="$1"
  local base_dir="$2"

  path="$(expand_home "$path")"
  case "$path" in
    /*)
      printf "%s\n" "$path"
      ;;
    *)
      if [[ -x "$path" ]]; then
        printf "%s\n" "$path"
      else
        printf "%s\n" "$base_dir/$path"
      fi
      ;;
  esac
}

parse_csv_strings() {
  local csv="$1"
  local value_name="$2"
  local -n out_ref="$3"

  IFS=',' read -r -a out_ref <<< "$csv"
  if [[ ${#out_ref[@]} -eq 0 ]]; then
    echo "No values in ${value_name}" >&2
    exit 1
  fi

  for i in "${!out_ref[@]}"; do
    local value="${out_ref[$i]//[[:space:]]/}"
    if [[ -z "$value" ]]; then
      echo "Empty value in ${value_name}" >&2
      exit 1
    fi
    out_ref[$i]="$value"
  done
}

parse_csv_uints() {
  local csv="$1"
  local value_name="$2"
  local allow_zero="$3"
  local -n out_ref="$4"

  parse_csv_strings "$csv" "$value_name" "$4"
  for i in "${!out_ref[@]}"; do
    local value="${out_ref[$i]}"
    if ! is_uint "$value"; then
      echo "Invalid value in ${value_name}: $value" >&2
      exit 1
    fi
    if [[ "$allow_zero" == "no" && "$value" -eq 0 ]]; then
      echo "${value_name} requires values > 0, got: $value" >&2
      exit 1
    fi
  done
}

parse_csv_floats() {
  local csv="$1"
  local value_name="$2"
  local -n out_ref="$3"

  parse_csv_strings "$csv" "$value_name" "$3"
  for i in "${!out_ref[@]}"; do
    local value="${out_ref[$i]}"
    if ! is_float "$value"; then
      echo "Invalid value in ${value_name}: $value" >&2
      exit 1
    fi
    out_ref[$i]="$(python3 - "$value" <<'PY'
import sys
print(f"{float(sys.argv[1]):.6f}")
PY
)"
  done
}

extract_metric() {
  local text="$1"
  local key="$2"
  awk -F': *' -v k="$key" '$1 == k {print $2; exit}' <<< "$text"
}

csv_escape() {
  local value="$1"
  if [[ "$value" == *","* || "$value" == *\"* || "$value" == *$'\n'* ]]; then
    value="${value//\"/\"\"}"
    printf '"%s"' "$value"
  else
    printf "%s" "$value"
  fi
}

join_csv_row() {
  local first="1"
  local value=""
  for value in "$@"; do
    if [[ "$first" == "1" ]]; then
      first="0"
    else
      printf ","
    fi
    csv_escape "$value"
  done
  printf "\n"
}

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 not found in PATH" >&2
  exit 1
fi

if ! is_uint "$duration_ms" || [[ "$duration_ms" -eq 0 ]]; then
  echo "--duration-ms must be an integer > 0" >&2
  exit 1
fi
if ! is_uint "$warmup_duration_ms"; then
  echo "--warmup-duration-ms must be an integer >= 0" >&2
  exit 1
fi
if ! is_uint "$timing_sample_stride" || [[ "$timing_sample_stride" -eq 0 ]]; then
  echo "--timing-sample-stride must be an integer > 0" >&2
  exit 1
fi
case "$timeslice_extension" in
  off|auto|require)
    ;;
  *)
    echo "--timeslice-extension must be one of: off, auto, require" >&2
    exit 1
    ;;
esac
if ! is_uint "$seed"; then
  echo "--seed must be an integer >= 0" >&2
  exit 1
fi
if ! is_uint "$repeats" || [[ "$repeats" -eq 0 ]]; then
  echo "--repeats must be an integer > 0" >&2
  exit 1
fi

declare -a lock_kinds=()
declare -a threads=()
declare -a lock_counts=()
declare -a zipf_alphas=()
declare -a critical_iters=()
declare -a outside_iters=()

parse_csv_strings "$lock_kinds_csv" "--lock-kinds" lock_kinds
parse_csv_uints "$threads_csv" "--threads" "no" threads
parse_csv_uints "$lock_counts_csv" "--lock-counts" "no" lock_counts
parse_csv_floats "$zipf_alpha_csv" "--zipf-alpha" zipf_alphas
parse_csv_uints "$critical_iters_csv" "--critical-ns" "yes" critical_iters
parse_csv_uints "$outside_iters_csv" "--outside-ns" "yes" outside_iters

binary="$(resolve_executable_path "$binary" "$MUTEXBENCH_DIR")"
output_root="$(resolve_path "$output_root" "$MUTEXBENCH_DIR")"
if [[ -z "$output_raw" ]]; then
  output_raw="$output_root/raw.csv"
else
  output_raw="$(resolve_path "$output_raw" "$MUTEXBENCH_DIR")"
fi
if [[ -z "$output_summary" ]]; then
  output_summary="$output_root/summary.csv"
else
  output_summary="$(resolve_path "$output_summary" "$MUTEXBENCH_DIR")"
fi

if [[ ! -x "$binary" && "$(basename "$binary")" == "multilockbench" ]]; then
  echo "Building multilockbench..." >&2
  make -C "$MUTEXBENCH_DIR" multilockbench >/dev/null
fi
if [[ ! -x "$binary" ]]; then
  echo "Benchmark binary is not executable: $binary" >&2
  exit 1
fi

mkdir -p "$output_root" "$(dirname "$output_raw")" "$(dirname "$output_summary")"

join_csv_row \
  "lock_kind" \
  "threads" \
  "lock_count" \
  "zipf_alpha" \
  "critical_iters" \
  "outside_iters" \
  "repeat" \
  "duration_ms" \
  "warmup_duration_ms" \
  "seed" \
  "throughput_ops_per_sec" \
  "elapsed_seconds" \
  "total_operations" \
  "avg_lock_hold_ns" \
  "avg_wait_ns_estimated" \
  "lock_hold_samples" \
  "hotspot_lock" \
  "hotspot_lock_operations" \
  "hotspot_lock_operation_pct" \
  "per_thread_operations" \
  "per_lock_operations" \
  > "$output_raw"

total_runs=$(( ${#lock_kinds[@]} * ${#threads[@]} * ${#lock_counts[@]} * ${#zipf_alphas[@]} * ${#critical_iters[@]} * ${#outside_iters[@]} * repeats ))
current_run=0

for lock_kind in "${lock_kinds[@]}"; do
  for thread_count in "${threads[@]}"; do
    for lock_count in "${lock_counts[@]}"; do
      for zipf_alpha in "${zipf_alphas[@]}"; do
        for critical_ns in "${critical_iters[@]}"; do
          for outside_ns in "${outside_iters[@]}"; do
            for ((repeat = 1; repeat <= repeats; ++repeat)); do
              current_run=$((current_run + 1))
              echo "[${current_run}/${total_runs}] lock_kind=${lock_kind} threads=${thread_count} locks=${lock_count} zipf_alpha=${zipf_alpha} critical=${critical_ns} outside=${outside_ns} repeat=${repeat}" >&2

              bench_cmd=(
                "$binary"
                --threads "$thread_count"
                --locks "$lock_count"
                --zipf-alpha "$zipf_alpha"
                --seed "$seed"
                --duration-ms "$duration_ms"
                --warmup-duration-ms "$warmup_duration_ms"
                --critical-ns "$critical_ns"
                --outside-ns "$outside_ns"
                --timing-sample-stride "$timing_sample_stride"
                --lock-kind "$lock_kind"
                --timeslice-extension "$timeslice_extension"
              )
              if [[ -n "$calibration_config" ]]; then
                bench_cmd+=( --calibration-config "$calibration_config" )
              fi

              bench_output="$("${bench_cmd[@]}")"

              throughput="$(extract_metric "$bench_output" "throughput_ops_per_sec")"
              elapsed_seconds="$(extract_metric "$bench_output" "elapsed_seconds")"
              total_operations="$(extract_metric "$bench_output" "total_operations")"
              avg_lock_hold_ns="$(extract_metric "$bench_output" "avg_lock_hold_ns")"
              avg_wait_ns_estimated="$(extract_metric "$bench_output" "avg_wait_ns_estimated")"
              lock_hold_samples="$(extract_metric "$bench_output" "lock_hold_samples")"
              hotspot_lock="$(extract_metric "$bench_output" "hotspot_lock")"
              hotspot_lock_operations="$(extract_metric "$bench_output" "hotspot_lock_operations")"
              hotspot_lock_operation_pct="$(extract_metric "$bench_output" "hotspot_lock_operation_pct")"
              per_thread_operations="$(extract_metric "$bench_output" "per_thread_operations")"
              per_lock_operations="$(extract_metric "$bench_output" "per_lock_operations")"

              if [[ -z "$throughput" || -z "$elapsed_seconds" || -z "$total_operations" || -z "$per_lock_operations" ]]; then
                echo "Failed to parse multilockbench output:" >&2
                echo "$bench_output" >&2
                exit 1
              fi

              per_thread_operations="${per_thread_operations//,/;}"
              per_lock_operations="${per_lock_operations//,/;}"

              join_csv_row \
                "$lock_kind" \
                "$thread_count" \
                "$lock_count" \
                "$zipf_alpha" \
                "$critical_ns" \
                "$outside_ns" \
                "$repeat" \
                "$duration_ms" \
                "$warmup_duration_ms" \
                "$seed" \
                "$throughput" \
                "$elapsed_seconds" \
                "$total_operations" \
                "$avg_lock_hold_ns" \
                "$avg_wait_ns_estimated" \
                "$lock_hold_samples" \
                "$hotspot_lock" \
                "$hotspot_lock_operations" \
                "$hotspot_lock_operation_pct" \
                "$per_thread_operations" \
                "$per_lock_operations" \
                >> "$output_raw"
            done
          done
        done
      done
    done
  done
done

python3 - "$output_raw" "$output_summary" <<'PY'
import csv
import sys
from collections import defaultdict

raw_path, summary_path = sys.argv[1], sys.argv[2]
group_fields = [
    "lock_kind",
    "threads",
    "lock_count",
    "zipf_alpha",
    "critical_iters",
    "outside_iters",
]
mean_fields = [
    ("throughput_ops_per_sec", "mean_throughput_ops_per_sec"),
    ("elapsed_seconds", "elapsed_seconds"),
    ("total_operations", "total_operations"),
    ("avg_lock_hold_ns", "avg_lock_hold_ns"),
    ("avg_wait_ns_estimated", "avg_wait_ns_estimated"),
    ("lock_hold_samples", "lock_hold_samples"),
    ("hotspot_lock_operation_pct", "mean_hotspot_lock_operation_pct"),
]

with open(raw_path, newline="") as f:
    rows = list(csv.DictReader(f))

groups = defaultdict(list)
for row in rows:
    groups[tuple(row[field] for field in group_fields)].append(row)

fieldnames = group_fields + [
    "repeats",
    "mean_throughput_ops_per_sec",
    "elapsed_seconds",
    "total_operations",
    "avg_lock_hold_ns",
    "avg_wait_ns_estimated",
    "lock_hold_samples",
    "mean_hotspot_lock_operation_pct",
]

with open(summary_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for key in sorted(groups):
        rows_for_key = groups[key]
        out = {field: value for field, value in zip(group_fields, key)}
        out["repeats"] = str(len(rows_for_key))
        for source, target in mean_fields:
            out[target] = f"{sum(float(row[source]) for row in rows_for_key) / len(rows_for_key):.6f}"
        writer.writerow(out)
PY

echo "Wrote raw CSV: $output_raw" >&2
echo "Wrote summary CSV: $output_summary" >&2
