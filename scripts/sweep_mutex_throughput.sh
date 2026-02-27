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
  --bench-ld-preload PATH      Set LD_PRELOAD only for benchmark binary execution
  --threads CSV                Thread counts, comma-separated (default: 1,2,4,8,16)
  --critical-iters CSV         critical-iters values (default: 10,50,100,200,500)
  --outside-iters CSV          outside-iters values (default: 10,50,100,200,500)
  --duration-ms N              measurement duration in ms (default: 1000)
  --warmup-duration-ms N       warmup duration in ms (default: 0)
  --timing-sample-stride N     timing sample stride (default: 8)
  --lock-kind K                lock kind: mutex|reciprocating|hapax|mcs|twa|clh (default: mutex)
  --repeats N                  runs per parameter point (default: 3)
  --output-raw PATH            raw per-run CSV (default: <mutexbench>/throughput_sweep_raw.csv)
  --output-summary PATH        aggregated CSV (default: <mutexbench>/throughput_sweep_summary.csv)
  -h, --help                   Show this help

Example:
  scripts/sweep_mutex_throughput.sh \
    --threads 1,2,4,8,16 \
    --critical-iters 10,100,500 \
    --outside-iters 10,100,500 \
    --duration-ms 1000 \
    --repeats 5 \
    --output-raw results/raw.csv \
    --output-summary results/summary.csv
EOF
}

binary="$MUTEXBENCH_DIR/mutex_bench"
bench_ld_preload=""
threads_csv="1,2,4,8,16,32,64"
critical_iters_csv="10,50,100,200,500,1000,2000"
outside_iters_csv="10,50,100,200,500,1000,2000"
duration_ms="2000"
warmup_duration_ms="50"
timing_sample_stride="8"
lock_kind="mutex"
repeats="3"
output_raw="$MUTEXBENCH_DIR/throughput_sweep_raw.csv"
output_summary="$MUTEXBENCH_DIR/throughput_sweep_summary.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --bench-ld-preload)
      bench_ld_preload="${2:-}"
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
    --lock-kind)
      lock_kind="${2:-}"
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

resolve_input_file_path() {
  local path="$1"
  local base_dir="$2"

  path="$(expand_home "$path")"
  case "$path" in
    /*)
      printf "%s\n" "$path"
      ;;
    *)
      if [[ -f "$path" ]]; then
        printf "%s\n" "$path"
      elif [[ -f "$base_dir/$path" ]]; then
        printf "%s\n" "$base_dir/$path"
      else
        printf "%s\n" "$path"
      fi
      ;;
  esac
}

restore_output_owner_if_sudo_user() {
  local sudo_uid="${SUDO_UID:-}"
  local sudo_gid="${SUDO_GID:-}"
  local path=""
  local parent=""

  if [[ "$EUID" -ne 0 || -z "$sudo_uid" || -z "$sudo_gid" ]]; then
    return 0
  fi

  for path in "$output_raw" "$output_summary"; do
    if [[ -e "$path" ]]; then
      if ! chown "$sudo_uid:$sudo_gid" "$path"; then
        echo "Warning: failed to chown file: $path" >&2
      fi
      if ! chmod u+rw "$path"; then
        echo "Warning: failed to chmod file: $path" >&2
      fi
    fi

    parent="$(dirname "$path")"
    if [[ -d "$parent" ]]; then
      if ! chown "$sudo_uid:$sudo_gid" "$parent"; then
        echo "Warning: failed to chown directory: $parent" >&2
      fi
      if ! chmod u+rwx "$parent"; then
        echo "Warning: failed to chmod directory: $parent" >&2
      fi
    fi
  done
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
case "$lock_kind" in
  mutex|reciprocating|hapax|mcs|twa|clh)
    ;;
  *)
    echo "--lock-kind must be one of: mutex, reciprocating, hapax, mcs, twa, clh" >&2
    exit 1
    ;;
esac
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

binary="$(resolve_executable_path "$binary" "$MUTEXBENCH_DIR")"
output_raw="$(resolve_output_path "$output_raw" "$MUTEXBENCH_DIR")"
output_summary="$(resolve_output_path "$output_summary" "$MUTEXBENCH_DIR")"
if [[ -n "$bench_ld_preload" ]]; then
  bench_ld_preload="$(resolve_input_file_path "$bench_ld_preload" "$MUTEXBENCH_DIR")"
  if [[ ! -f "$bench_ld_preload" ]]; then
    echo "--bench-ld-preload file not found: $bench_ld_preload" >&2
    exit 1
  fi
fi

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
  for c in "${critical_iters[@]}"; do
    for o in "${outside_iters[@]}"; do
      for ((r = 1; r <= repeats; ++r)); do
        current_run=$((current_run + 1))
        echo "[${current_run}/${total_runs}] lock_kind=${lock_kind} threads=${t} critical=${c} outside=${o} repeat=${r} duration_ms=${duration_ms} warmup_duration_ms=${warmup_duration_ms}" >&2

        bench_cmd=(
          "$binary"
          --threads "$t"
          --lock-kind "$lock_kind"
          --duration-ms "$duration_ms"
          --warmup-duration-ms "$warmup_duration_ms"
          --critical-iters "$c"
          --outside-iters "$o"
          --timing-sample-stride "$timing_sample_stride"
        )
        if [[ -n "$bench_ld_preload" ]]; then
          bench_output="$(env LD_PRELOAD="$bench_ld_preload" "${bench_cmd[@]}")"
        else
          bench_output="$("${bench_cmd[@]}")"
        fi

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

    if ($6 != "")  { sum_elapsed[key] += ($6 + 0.0); cnt_elapsed[key]++ }
    if ($7 != "")  { sum_total_ops[key] += ($7 + 0.0); cnt_total_ops[key]++ }
    if ($8 != "")  { sum_waiters[key] += ($8 + 0.0); cnt_waiters[key]++ }
    if ($9 != "")  { sum_lock_hold_ns[key] += ($9 + 0.0); cnt_lock_hold_ns[key]++ }
    if ($10 != "") { sum_u2n_all[key] += ($10 + 0.0); cnt_u2n_all[key]++ }
    if ($11 != "") { sum_counter[key] += ($11 + 0.0); cnt_counter[key]++ }
    if ($12 != "") { sum_lock_hold_samples[key] += ($12 + 0.0); cnt_lock_hold_samples[key]++ }
    if ($13 != "") { sum_u2n_w0_samples[key] += ($13 + 0.0); cnt_u2n_w0_samples[key]++ }
    if ($14 != "") { sum_u2n_w0_ns[key] += ($14 + 0.0); cnt_u2n_w0_ns[key]++ }
    if ($15 != "") { sum_u2n_wgt0_samples[key] += ($15 + 0.0); cnt_u2n_wgt0_samples[key]++ }
    if ($16 != "") { sum_u2n_wgt0_ns[key] += ($16 + 0.0); cnt_u2n_wgt0_ns[key]++ }
  }
  END {
    print "threads,critical_iters,outside_iters,repeats,mean_throughput_ops_per_sec,stddev_throughput_ops_per_sec,min_throughput_ops_per_sec,max_throughput_ops_per_sec,mean_elapsed_seconds,mean_total_operations,mean_avg_waiters_before_lock,mean_avg_lock_hold_ns,mean_avg_unlock_to_next_lock_ns_all,mean_protected_counter,mean_lock_hold_samples,mean_unlock_to_next_lock_samples_w0,mean_avg_unlock_to_next_lock_ns_w0,mean_unlock_to_next_lock_samples_w_gt0,mean_avg_unlock_to_next_lock_ns_w_gt0"
    for (k in n) {
      mean = sum[k] / n[k]
      var = (sumsq[k] / n[k]) - (mean * mean)
      if (var < 0) var = 0
      std = sqrt(var)

      mean_elapsed = (cnt_elapsed[k] > 0) ? (sum_elapsed[k] / cnt_elapsed[k]) : 0
      mean_total_ops = (cnt_total_ops[k] > 0) ? (sum_total_ops[k] / cnt_total_ops[k]) : 0
      mean_waiters = (cnt_waiters[k] > 0) ? (sum_waiters[k] / cnt_waiters[k]) : 0
      mean_lock_hold = (cnt_lock_hold_ns[k] > 0) ? (sum_lock_hold_ns[k] / cnt_lock_hold_ns[k]) : 0
      mean_u2n_all = (cnt_u2n_all[k] > 0) ? (sum_u2n_all[k] / cnt_u2n_all[k]) : 0
      mean_counter = (cnt_counter[k] > 0) ? (sum_counter[k] / cnt_counter[k]) : 0
      mean_lock_hold_samples = (cnt_lock_hold_samples[k] > 0) ? (sum_lock_hold_samples[k] / cnt_lock_hold_samples[k]) : 0
      mean_u2n_w0_samples = (cnt_u2n_w0_samples[k] > 0) ? (sum_u2n_w0_samples[k] / cnt_u2n_w0_samples[k]) : 0
      mean_u2n_w0_ns = (cnt_u2n_w0_ns[k] > 0) ? (sum_u2n_w0_ns[k] / cnt_u2n_w0_ns[k]) : 0
      mean_u2n_wgt0_samples = (cnt_u2n_wgt0_samples[k] > 0) ? (sum_u2n_wgt0_samples[k] / cnt_u2n_wgt0_samples[k]) : 0
      mean_u2n_wgt0_ns = (cnt_u2n_wgt0_ns[k] > 0) ? (sum_u2n_wgt0_ns[k] / cnt_u2n_wgt0_ns[k]) : 0

      split(k, parts, FS)
      printf "%s,%s,%s,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f\n", \
             parts[1], parts[2], parts[3], n[k], mean, std, min[k], max[k], \
             mean_elapsed, mean_total_ops, mean_waiters, mean_lock_hold, mean_u2n_all, \
             mean_counter, mean_lock_hold_samples, mean_u2n_w0_samples, mean_u2n_w0_ns, \
             mean_u2n_wgt0_samples, mean_u2n_wgt0_ns
    }
  }
' "$output_raw" | sort -t',' -k1,1n -k2,2n -k3,3n > "$output_summary"

restore_output_owner_if_sudo_user

echo "Raw results: $output_raw" >&2
echo "Summary results: $output_summary" >&2
