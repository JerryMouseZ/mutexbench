#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Sweep mutex_bench throughput across:
  - --threads
  - --critical-iters
  - --outside-iters

Usage:
  scripts/sweep_mutex_throughput.sh [options]

Options:
  --binary PATH                Benchmark binary path (default: ./mutex_bench)
  --threads CSV                Thread counts, comma-separated (default: 1,2,4,8,16)
  --critical-iters CSV         critical-iters values (default: 10,50,100,200,500)
  --outside-iters CSV          outside-iters values (default: 10,50,100,200,500)
  --iterations N               iterations per thread (default: 200000)
  --warmup-iterations N        warmup iterations per thread (default: 20000)
  --timing-sample-stride N     timing sample stride (default: 8)
  --repeats N                  runs per parameter point (default: 3)
  --output-raw PATH            raw per-run CSV (default: throughput_sweep_raw.csv)
  --output-summary PATH        aggregated CSV (default: throughput_sweep_summary.csv)
  -h, --help                   Show this help

Example:
  scripts/sweep_mutex_throughput.sh \
    --threads 1,2,4,8,16 \
    --critical-iters 10,100,500 \
    --outside-iters 10,100,500 \
    --iterations 300000 \
    --repeats 5 \
    --output-raw results/raw.csv \
    --output-summary results/summary.csv
EOF
}

binary="./mutex_bench"
threads_csv="1,2,4,8,16"
critical_iters_csv="10,50,100,200,500"
outside_iters_csv="10,50,100,200,500"
iterations="200000"
warmup_iterations="20000"
timing_sample_stride="8"
repeats="3"
output_raw="throughput_sweep_raw.csv"
output_summary="throughput_sweep_summary.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --threads)
      threads_csv="${2:-}"
      shift 2
      ;;
    --critical-iters)
      critical_iters_csv="${2:-}"
      shift 2
      ;;
    --outside-iters)
      outside_iters_csv="${2:-}"
      shift 2
      ;;
    --iterations)
      iterations="${2:-}"
      shift 2
      ;;
    --warmup-iterations)
      warmup_iterations="${2:-}"
      shift 2
      ;;
    --timing-sample-stride)
      timing_sample_stride="${2:-}"
      shift 2
      ;;
    --repeats)
      repeats="${2:-}"
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

parse_csv_values() {
  local csv="$1"
  local value_name="$2"
  local allow_zero="$3"
  local -n output_array="$4"

  IFS=',' read -r -a output_array <<< "$csv"
  if [[ ${#output_array[@]} -eq 0 ]]; then
    echo "No values in ${value_name}" >&2
    exit 1
  fi

  for i in "${!output_array[@]}"; do
    local v="${output_array[$i]//[[:space:]]/}"
    if [[ -z "$v" ]]; then
      echo "Empty value in ${value_name}" >&2
      exit 1
    fi
    if ! is_uint "$v"; then
      echo "Invalid value in ${value_name}: $v" >&2
      exit 1
    fi
    if [[ "$allow_zero" == "no" && "$v" -eq 0 ]]; then
      echo "${value_name} requires values > 0, got: $v" >&2
      exit 1
    fi
    output_array[$i]="$v"
  done
}

if ! is_uint "$iterations" || [[ "$iterations" -eq 0 ]]; then
  echo "--iterations must be an integer > 0" >&2
  exit 1
fi
if ! is_uint "$warmup_iterations"; then
  echo "--warmup-iterations must be an integer >= 0" >&2
  exit 1
fi
if ! is_uint "$timing_sample_stride" || [[ "$timing_sample_stride" -eq 0 ]]; then
  echo "--timing-sample-stride must be an integer > 0" >&2
  exit 1
fi
if ! is_uint "$repeats" || [[ "$repeats" -eq 0 ]]; then
  echo "--repeats must be an integer > 0" >&2
  exit 1
fi

declare -a threads=()
declare -a critical_iters=()
declare -a outside_iters=()

parse_csv_values "$threads_csv" "--threads" "no" threads
parse_csv_values "$critical_iters_csv" "--critical-iters" "yes" critical_iters
parse_csv_values "$outside_iters_csv" "--outside-iters" "yes" outside_iters

if [[ ! -x "$binary" && "$(basename "$binary")" == "mutex_bench" ]]; then
  echo "Building mutex_bench..." >&2
  make mutex_bench >/dev/null
fi

if [[ ! -x "$binary" ]]; then
  echo "Benchmark binary is not executable: $binary" >&2
  exit 1
fi

mkdir -p "$(dirname "$output_raw")"
mkdir -p "$(dirname "$output_summary")"

printf "%s\n" \
  "threads,critical_iters,outside_iters,repeat,throughput_ops_per_sec,elapsed_seconds,total_operations,avg_waiters_before_lock,avg_lock_hold_ns,avg_unlock_to_next_lock_ns_all" \
  > "$output_raw"

extract_metric() {
  local text="$1"
  local key="$2"
  awk -F': *' -v k="$key" '$1 == k {print $2; exit}' <<< "$text"
}

total_runs=$(( ${#threads[@]} * ${#critical_iters[@]} * ${#outside_iters[@]} * repeats ))
current_run=0

for t in "${threads[@]}"; do
  for c in "${critical_iters[@]}"; do
    for o in "${outside_iters[@]}"; do
      for ((r = 1; r <= repeats; ++r)); do
        current_run=$((current_run + 1))
        echo "[${current_run}/${total_runs}] threads=${t} critical=${c} outside=${o} repeat=${r}" >&2

        bench_output="$("$binary" \
          --threads "$t" \
          --iterations "$iterations" \
          --warmup-iterations "$warmup_iterations" \
          --critical-iters "$c" \
          --outside-iters "$o" \
          --timing-sample-stride "$timing_sample_stride")"

        throughput="$(extract_metric "$bench_output" "throughput_ops_per_sec")"
        elapsed_seconds="$(extract_metric "$bench_output" "elapsed_seconds")"
        total_operations="$(extract_metric "$bench_output" "total_operations")"
        avg_waiters_before_lock="$(extract_metric "$bench_output" "avg_waiters_before_lock")"
        avg_lock_hold_ns="$(extract_metric "$bench_output" "avg_lock_hold_ns")"
        avg_unlock_to_next_lock_ns_all="$(extract_metric "$bench_output" "avg_unlock_to_next_lock_ns_all")"

        if [[ -z "$throughput" || -z "$elapsed_seconds" || -z "$total_operations" ]]; then
          echo "Failed to parse benchmark output for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
          echo "$bench_output" >&2
          exit 1
        fi

        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
          "$t" "$c" "$o" "$r" \
          "$throughput" "$elapsed_seconds" "$total_operations" \
          "$avg_waiters_before_lock" "$avg_lock_hold_ns" "$avg_unlock_to_next_lock_ns_all" \
          >> "$output_raw"
      done
    done
  done
done

awk -F',' '
  NR == 1 { next }
  {
    key = $1 FS $2 FS $3
    n[key]++
    x = $5 + 0.0
    sum[key] += x
    sumsq[key] += x * x
    if (!(key in min) || x < min[key]) min[key] = x
    if (!(key in max) || x > max[key]) max[key] = x
  }
  END {
    print "threads,critical_iters,outside_iters,repeats,mean_throughput_ops_per_sec,stddev_throughput_ops_per_sec,min_throughput_ops_per_sec,max_throughput_ops_per_sec"
    for (k in n) {
      mean = sum[k] / n[k]
      var = (sumsq[k] / n[k]) - (mean * mean)
      if (var < 0) var = 0
      std = sqrt(var)
      split(k, parts, FS)
      printf "%s,%s,%s,%d,%.6f,%.6f,%.6f,%.6f\n", \
             parts[1], parts[2], parts[3], n[k], mean, std, min[k], max[k]
    }
  }
' "$output_raw" | sort -t',' -k1,1n -k2,2n -k3,3n > "$output_summary"

echo "Raw results: $output_raw" >&2
echo "Summary results: $output_summary" >&2
