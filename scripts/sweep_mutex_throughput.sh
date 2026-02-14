#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Sweep mutex_bench throughput across:
  - --threads
  - --critical-iters
  - --outside-iters

Usage:
  scripts/sweep_mutex_throughput.sh [options]

Options:
  --binary PATH                Benchmark binary path (default: <mutexbench>/mutex_bench)
  --threads CSV                Thread counts, comma-separated (default: 1,2,4,8,16)
  --critical-iters CSV         critical-iters values (default: 10,50,100,200,500)
  --outside-iters CSV          outside-iters values (default: 10,50,100,200,500)
  --iterations N               base iterations per thread (default: 200000)
  --warmup-iterations N        base warmup iterations per thread (default: 50000)
  --scale-workload-with-threads yes|no
                               Scale per-thread iterations by thread count (default: yes)
  --min-iterations-per-thread N
                               Minimum per-thread iterations after scaling (default: 1000)
  --min-warmup-iterations-per-thread N
                               Minimum per-thread warmup iterations after scaling (default: 0)
  --timing-sample-stride N     timing sample stride (default: 8)
  --repeats N                  runs per parameter point (default: 3)
  --output-raw PATH            raw per-run CSV (default: <mutexbench>/throughput_sweep_raw.csv)
  --output-summary PATH        aggregated CSV (default: <mutexbench>/throughput_sweep_summary.csv)
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

binary="$MUTEXBENCH_DIR/mutex_bench"
threads_csv="1,2,4,8,16,32,64"
critical_iters_csv="10,50,100,200,500,1000,2000"
outside_iters_csv="10,50,100,200,500,1000,2000"
iterations="409600"
warmup_iterations="50000"
timing_sample_stride="8"
repeats="5"
output_raw="$MUTEXBENCH_DIR/throughput_sweep_raw.csv"
output_summary="$MUTEXBENCH_DIR/throughput_sweep_summary.csv"
scale_workload_with_threads="yes"
min_iterations_per_thread="1000"
min_warmup_iterations_per_thread="0"

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
    --scale-workload-with-threads)
      scale_workload_with_threads="${2:-}"
      shift 2
      ;;
    --min-iterations-per-thread)
      min_iterations_per_thread="${2:-}"
      shift 2
      ;;
    --min-warmup-iterations-per-thread)
      min_warmup_iterations_per_thread="${2:-}"
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

resolve_output_path() {
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
if [[ "$scale_workload_with_threads" != "yes" && "$scale_workload_with_threads" != "no" ]]; then
  echo "--scale-workload-with-threads must be 'yes' or 'no'" >&2
  exit 1
fi
if ! is_uint "$min_iterations_per_thread" || [[ "$min_iterations_per_thread" -eq 0 ]]; then
  echo "--min-iterations-per-thread must be an integer > 0" >&2
  exit 1
fi
if ! is_uint "$min_warmup_iterations_per_thread"; then
  echo "--min-warmup-iterations-per-thread must be an integer >= 0" >&2
  exit 1
fi

declare -a threads=()
declare -a critical_iters=()
declare -a outside_iters=()

parse_csv_values "$threads_csv" "--threads" "no" threads
parse_csv_values "$critical_iters_csv" "--critical-iters" "yes" critical_iters
parse_csv_values "$outside_iters_csv" "--outside-iters" "yes" outside_iters

binary="$(resolve_executable_path "$binary" "$MUTEXBENCH_DIR")"
output_raw="$(resolve_output_path "$output_raw" "$MUTEXBENCH_DIR")"
output_summary="$(resolve_output_path "$output_summary" "$MUTEXBENCH_DIR")"

scaled_per_thread_work() {
  local base_per_thread="$1"
  local thread_count="$2"
  local minimum_per_thread="$3"
  local scaled="$base_per_thread"

  if [[ "$scale_workload_with_threads" == "yes" ]]; then
    scaled=$(( (base_per_thread + thread_count - 1) / thread_count ))
    if [[ "$scaled" -lt "$minimum_per_thread" ]]; then
      scaled="$minimum_per_thread"
    fi
  fi

  printf "%s\n" "$scaled"
}

if [[ ! -x "$binary" && "$(basename "$binary")" == "mutex_bench" ]]; then
  echo "Building mutex_bench..." >&2
  make -C "$MUTEXBENCH_DIR" mutex_bench >/dev/null
fi

if [[ ! -x "$binary" ]]; then
  echo "Benchmark binary is not executable: $binary" >&2
  exit 1
fi

mkdir -p "$(dirname "$output_raw")"
mkdir -p "$(dirname "$output_summary")"

printf "%s\n" \
  "threads,critical_iters,outside_iters,repeat,throughput_ops_per_sec,elapsed_seconds,total_operations,avg_waiters_before_lock,avg_lock_hold_ns,avg_unlock_to_next_lock_ns_all,protected_counter,lock_hold_samples,unlock_to_next_lock_samples_w0,avg_unlock_to_next_lock_ns_w0,unlock_to_next_lock_samples_w_gt0,avg_unlock_to_next_lock_ns_w_gt0" \
  > "$output_raw"

extract_metric() {
  local text="$1"
  local key="$2"
  awk -F': *' -v k="$key" '$1 == k {print $2; exit}' <<< "$text"
}

total_runs=$(( ${#threads[@]} * ${#critical_iters[@]} * ${#outside_iters[@]} * repeats ))
current_run=0

for t in "${threads[@]}"; do
  scaled_iterations="$(scaled_per_thread_work "$iterations" "$t" "$min_iterations_per_thread")"
  scaled_warmup_iterations="$(
    scaled_per_thread_work "$warmup_iterations" "$t" "$min_warmup_iterations_per_thread"
  )"
  for c in "${critical_iters[@]}"; do
    for o in "${outside_iters[@]}"; do
      for ((r = 1; r <= repeats; ++r)); do
        current_run=$((current_run + 1))
        echo "[${current_run}/${total_runs}] threads=${t} critical=${c} outside=${o} repeat=${r} iterations_per_thread=${scaled_iterations} warmup_iterations_per_thread=${scaled_warmup_iterations}" >&2

        bench_output="$("$binary" \
          --threads "$t" \
          --iterations "$scaled_iterations" \
          --warmup-iterations "$scaled_warmup_iterations" \
          --critical-iters "$c" \
          --outside-iters "$o" \
          --timing-sample-stride "$timing_sample_stride")"

        # Initialize all per-run fields so nounset never trips on missing metrics.
        throughput=""
        elapsed_seconds=""
        total_operations=""
        avg_waiters_before_lock=""
        avg_lock_hold_ns=""
        avg_unlock_to_next_lock_ns_all=""
        protected_counter=""
        lock_hold_samples=""
        unlock_to_next_lock_samples_w0=""
        avg_unlock_to_next_lock_ns_w0=""
        unlock_to_next_lock_samples_w_gt0=""
        avg_unlock_to_next_lock_ns_w_gt0=""

        throughput="$(extract_metric "$bench_output" "throughput_ops_per_sec")"
        elapsed_seconds="$(extract_metric "$bench_output" "elapsed_seconds")"
        total_operations="$(extract_metric "$bench_output" "total_operations")"
        avg_waiters_before_lock="$(extract_metric "$bench_output" "avg_waiters_before_lock")"
        avg_lock_hold_ns="$(extract_metric "$bench_output" "avg_lock_hold_ns")"
        avg_unlock_to_next_lock_ns_all="$(extract_metric "$bench_output" "avg_unlock_to_next_lock_ns_all")"
        protected_counter="$(extract_metric "$bench_output" "protected_counter")"
        lock_hold_samples="$(extract_metric "$bench_output" "lock_hold_samples")"
        unlock_to_next_lock_samples_w0="$(extract_metric "$bench_output" "unlock_to_next_lock_samples_w0")"
        avg_unlock_to_next_lock_ns_w0="$(extract_metric "$bench_output" "avg_unlock_to_next_lock_ns_w0")"
        unlock_to_next_lock_samples_w_gt0="$(extract_metric "$bench_output" "unlock_to_next_lock_samples_w_gt0")"
        avg_unlock_to_next_lock_ns_w_gt0="$(extract_metric "$bench_output" "avg_unlock_to_next_lock_ns_w_gt0")"

        if [[ -z "$throughput" || -z "$elapsed_seconds" || -z "$total_operations" ]]; then
          echo "Failed to parse benchmark output for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
          echo "$bench_output" >&2
          exit 1
        fi

        printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" \
          "$t" "$c" "$o" "$r" \
          "$throughput" "$elapsed_seconds" "$total_operations" \
          "$avg_waiters_before_lock" "$avg_lock_hold_ns" "$avg_unlock_to_next_lock_ns_all" \
          "$protected_counter" "$lock_hold_samples" \
          "$unlock_to_next_lock_samples_w0" "$avg_unlock_to_next_lock_ns_w0" \
          "$unlock_to_next_lock_samples_w_gt0" "$avg_unlock_to_next_lock_ns_w_gt0" \
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
