#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Sweep mutex_bench throughput across:
  - --threads
  - --critical-ns
  - --outside-ns

Usage:
  scripts/sweep_mutex_throughput.sh [options]

Options:
  --binary PATH                Benchmark binary path (default: <mutexbench>/mutex_bench)
  --bench-ld-preload PATH      Set LD_PRELOAD only for benchmark binary execution
  --calibration-config PATH    Pass an explicit iter calibration config to mutex_bench
  --threads CSV                Thread counts, comma-separated (default: 1,2,4,8,16)
  --critical-ns CSV            Critical-section burn time in ns (default: 10,50,100,200,500)
  --outside-ns CSV             Non-critical-section burn time in ns (default: 10,50,100,200,500)
  --critical-iters CSV         Legacy alias for --critical-ns
  --duration-ms N              measurement duration in ms (default: 1000)
  --warmup-duration-ms N       warmup duration in ms (default: 0)
  --timing-sample-stride N     timing sample stride (default: 8)
  --lock-kind K                lock kind: mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh (default: mutex)
  --timeslice-extension M      off|auto|require (default: off; ignored for *-tse lock kinds)
  --repeats N                  runs per parameter point (default: 3)
  --profile                    Record perf.data for each run and keep it beside raw.csv
  --sample-bpf                 Record per-run lb_simple BPF sampler CSV beside raw.csv
  --sample-bpf-layout MODE     Sampler layout: auto|v1|v2|legacy|current (default: auto)
  --sample-bpf-interval-us N   Sampler interval in microseconds (default: 500)
  --output-raw PATH            raw per-run CSV (default: <mutexbench>/throughput_sweep_raw.csv)
  --output-summary PATH        aggregated CSV (default: <mutexbench>/throughput_sweep_summary.csv)
  -h, --help                   Show this help

Example:
  scripts/sweep_mutex_throughput.sh \
    --threads 1,2,4,8,16 \
    --critical-ns 10,100,500 \
    --outside-ns 10,100,500 \
    --duration-ms 1000 \
    --repeats 5 \
    --output-raw results/raw.csv \
    --output-summary results/summary.csv
EOF
}

binary="$MUTEXBENCH_DIR/mutex_bench"
bench_ld_preload=""
calibration_config=""
threads_csv="1,2,4,8,16,32,64"
critical_iters_csv="10,50,100,200,500,1000,2000"
outside_iters_csv="10,50,100,200,500,1000,2000"
duration_ms="2000"
warmup_duration_ms="50"
timing_sample_stride="8"
lock_kind="mutex"
timeslice_extension="off"
repeats="3"
profiling_enabled="0"
sample_bpf_enabled="0"
sample_bpf_layout="auto"
sample_bpf_interval_us="500"
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
    --calibration-config)
      calibration_config="${2:-}"
      shift 2
      ;;
    --threads)
      threads_csv="${2:-}"
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
    --lock-kind)
      lock_kind="${2:-}"
      shift 2
      ;;
    --timeslice-extension)
      timeslice_extension="${2:-}"
      shift 2
      ;;
    --repeats)
      repeats="${2:-}"
      shift 2
      ;;
    --profile)
      if [[ $# -gt 1 && -n "${2:-}" && "${2:0:1}" != "-" ]]; then
        echo "--profile does not take a value; use bare --profile" >&2
        exit 1
      fi
      profiling_enabled="1"
      shift
      ;;
    --sample-bpf)
      if [[ $# -gt 1 && -n "${2:-}" && "${2:0:1}" != "-" ]]; then
        echo "--sample-bpf does not take a value; use bare --sample-bpf" >&2
        exit 1
      fi
      sample_bpf_enabled="1"
      shift
      ;;
    --sample-bpf-layout)
      sample_bpf_layout="${2:-}"
      shift 2
      ;;
    --sample-bpf-interval-us)
      sample_bpf_interval_us="${2:-}"
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

  for path in "$@"; do
    [[ -z "$path" ]] && continue

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
  mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh)
    ;;
  *)
    echo "--lock-kind must be one of: mutex, reciprocating, hapax, mcs, mcs-tas, mcs-tas-tse, mcstas-next, mcstas-next-tse, twa, clh" >&2
    exit 1
    ;;
esac
case "$timeslice_extension" in
  off|auto|require)
    ;;
  *)
    echo "--timeslice-extension must be one of: off, auto, require" >&2
    exit 1
    ;;
esac

lock_uses_builtin_timeslice_extension="0"
case "$lock_kind" in
  mcs-tas-tse|mcstas-next-tse)
    lock_uses_builtin_timeslice_extension="1"
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
parse_csv_values "$critical_iters_csv" "--critical-ns" "yes" critical_iters
parse_csv_values "$outside_iters_csv" "--outside-ns" "yes" outside_iters

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

if ! command -v pidstat >/dev/null 2>&1; then
  echo "pidstat not found in PATH" >&2
  exit 1
fi
if [[ "$profiling_enabled" == "1" ]] && ! command -v perf >/dev/null 2>&1; then
  echo "perf not found in PATH" >&2
  exit 1
fi
sample_bpf_script="$SCRIPT_DIR/sample_lb_simple_bpf.py"
if [[ "$sample_bpf_enabled" == "1" ]]; then
  if ! is_uint "$sample_bpf_interval_us" || [[ "$sample_bpf_interval_us" -le 0 ]]; then
    echo "--sample-bpf-interval-us must be a positive integer" >&2
    exit 1
  fi
  case "$sample_bpf_layout" in
    auto|v1|v2|legacy|current)
      ;;
    *)
      echo "--sample-bpf-layout must be one of: auto, v1, v2, legacy, current" >&2
      exit 1
      ;;
  esac
  if ! command -v python3 >/dev/null 2>&1; then
    echo "python3 not found in PATH" >&2
    exit 1
  fi
  if [[ ! -f "$sample_bpf_script" ]]; then
    echo "BPF sampler script not found: $sample_bpf_script" >&2
    exit 1
  fi
  if [[ "$EUID" -ne 0 ]]; then
    echo "--sample-bpf requires running sweep_mutex_throughput.sh as root (or via sudo)." >&2
    exit 1
  fi
fi

raw_output_dir="$(dirname "$output_raw")"
mkdir -p "$raw_output_dir"
mkdir -p "$(dirname "$output_summary")"

raw_header="threads,critical_iters,outside_iters,repeat,throughput_ops_per_sec,elapsed_seconds,total_operations,avg_lock_hold_ns,avg_wait_ns_estimated,avg_lock_handoff_ns_estimated,lock_hold_samples,avg_cpu_pct"
if [[ "$profiling_enabled" == "1" ]]; then
  raw_header+=",perf_data_path"
fi
if [[ "$sample_bpf_enabled" == "1" ]]; then
  raw_header+=",bpf_samples_path,bpf_layout,bpf_interval_us"
fi
printf "%s\n" "$raw_header" > "$output_raw"

extract_metric() {
  local text="$1"
  local key="$2"
  awk -F': *' -v k="$key" '$1 == k {print $2; exit}' <<< "$text"
}

extract_avg_cpu_pct() {
  local pidstat_output_path="$1"

  awk '
    function is_float(value) {
      return value ~ /^-?[0-9]+([.][0-9]+)?$/
    }

    function is_meridiem(value) {
      return value == "AM" || value == "PM"
    }

    $1 ~ /^[0-9][0-9]:[0-9][0-9]:[0-9][0-9]$/ {
      pid_field = 3
      cpu_field = 8
      if (is_meridiem($2)) {
        pid_field = 4
        cpu_field = 9
      }

      if ($(pid_field) ~ /^[0-9]+$/ && is_float($(cpu_field))) {
        pid = $(pid_field)
        counts[pid] += 1
        samples[pid SUBSEP counts[pid]] = $(cpu_field) + 0.0
      }
    }

    END {
      total = 0.0
      kept = 0
      for (pid in counts) {
        start = (counts[pid] > 1) ? 2 : 1
        for (i = start; i <= counts[pid]; ++i) {
          total += samples[pid SUBSEP i]
          kept += 1
        }
      }
      if (kept == 0) {
        exit 1
      }
      printf "%.6f\n", total / kept
    }
  ' "$pidstat_output_path"
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
          --critical-ns "$c"
          --outside-ns "$o"
          --timing-sample-stride "$timing_sample_stride"
        )
        if [[ "$lock_uses_builtin_timeslice_extension" != "1" ]]; then
          bench_cmd+=( --timeslice-extension "$timeslice_extension" )
        fi
        if [[ -n "$calibration_config" ]]; then
          bench_cmd+=( --calibration-config "$calibration_config" )
        fi

        # Initialize per-run fields so nounset never trips on missing metrics.
        throughput=""
        elapsed_seconds=""
        total_operations=""
        avg_lock_hold_ns=""
        avg_wait_ns_estimated=""
        avg_lock_handoff_ns_estimated=""
        lock_hold_samples=""
        avg_cpu_pct=""

        bench_output_path="$(mktemp)"
        pidstat_output_path="$(mktemp)"
        perf_data_path=""
        bpf_samples_path=""
        bpf_sampler_pid=""
        bpf_sampler_log_path=""

        if [[ "$profiling_enabled" == "1" ]]; then
          perf_data_path="$raw_output_dir/t${t}_c${c}_o${o}_r${r}.perf.data"
          if [[ -n "$bench_ld_preload" ]]; then
            perf record -q -F 499 -e cpu-clock -o "$perf_data_path" -- env LD_PRELOAD="$bench_ld_preload" "${bench_cmd[@]}" >"$bench_output_path" &
          else
            perf record -q -F 499 -e cpu-clock -o "$perf_data_path" -- "${bench_cmd[@]}" >"$bench_output_path" &
          fi
        elif [[ -n "$bench_ld_preload" ]]; then
          env LD_PRELOAD="$bench_ld_preload" "${bench_cmd[@]}" >"$bench_output_path" &
        else
          "${bench_cmd[@]}" >"$bench_output_path" &
        fi
        bench_pid=$!

        env LC_ALL=C pidstat -u -h -p "$bench_pid" 1 >"$pidstat_output_path" 2>&1 &
        pidstat_pid=$!

        if [[ "$sample_bpf_enabled" == "1" ]]; then
          bpf_samples_path="$raw_output_dir/t${t}_c${c}_o${o}_r${r}.bpf_samples.csv"
          bpf_sampler_log_path="$raw_output_dir/t${t}_c${c}_o${o}_r${r}.bpf_samples.stderr"
          sample_duration_s="$(awk -v warmup_ms="$warmup_duration_ms" -v duration_ms="$duration_ms" 'BEGIN { printf "%.3f", (warmup_ms + duration_ms) / 1000.0 + 0.250 }')"
          sched_ext_ready="0"
          for _ in $(seq 1 80); do
            if [[ -r /sys/kernel/sched_ext/state ]] && [[ "$(< /sys/kernel/sched_ext/state)" == "enabled" ]]; then
              sched_ext_ready="1"
              break
            fi
            sleep 0.05
          done
          if [[ "$sched_ext_ready" != "1" ]]; then
            echo "Timed out waiting for sched_ext to enable before starting BPF sampler" >&2
            exit 1
          fi
          python3 "$sample_bpf_script" \
            --layout "$sample_bpf_layout" \
            --duration-s "$sample_duration_s" \
            --interval-us "$sample_bpf_interval_us" \
            --include-agg \
            --include-slots \
            --slot-limit 64 \
            --output "$bpf_samples_path" \
            > /dev/null 2>"$bpf_sampler_log_path" &
          bpf_sampler_pid=$!
        fi

        if wait "$bench_pid"; then
          bench_status=0
        else
          bench_status=$?
        fi

        kill "$pidstat_pid" >/dev/null 2>&1 || true
        wait "$pidstat_pid" >/dev/null 2>&1 || true
        if [[ -n "$bpf_sampler_pid" ]]; then
          sampler_done="0"
          for _ in $(seq 1 20); do
            if ! kill -0 "$bpf_sampler_pid" >/dev/null 2>&1; then
              sampler_done="1"
              break
            fi
            sleep 0.05
          done
          if [[ "$sampler_done" != "1" ]]; then
            kill "$bpf_sampler_pid" >/dev/null 2>&1 || true
          fi
          wait "$bpf_sampler_pid" >/dev/null 2>&1 || true
        fi

        bench_output="$(<"$bench_output_path")"

        if [[ "$bench_status" -ne 0 ]]; then
          echo "Benchmark command failed for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
          echo "$bench_output" >&2
          rm -f -- "$bench_output_path" "$pidstat_output_path"
          exit "$bench_status"
        fi

        throughput="$(extract_metric "$bench_output" "throughput_ops_per_sec")"
        elapsed_seconds="$(extract_metric "$bench_output" "elapsed_seconds")"
        total_operations="$(extract_metric "$bench_output" "total_operations")"
        avg_lock_hold_ns="$(extract_metric "$bench_output" "avg_lock_hold_ns")"
        avg_wait_ns_estimated="$(extract_metric "$bench_output" "avg_wait_ns_estimated")"
        avg_lock_handoff_ns_estimated="$(extract_metric "$bench_output" "avg_lock_handoff_ns_estimated")"
        lock_hold_samples="$(extract_metric "$bench_output" "lock_hold_samples")"

        if ! avg_cpu_pct="$(extract_avg_cpu_pct "$pidstat_output_path")"; then
          echo "Failed to parse steady CPU samples for threads=${t} critical=${c} outside=${o} repeat=${r}; ensure pidstat emitted at least one sample" >&2
          cat "$pidstat_output_path" >&2
          rm -f -- "$bench_output_path" "$pidstat_output_path"
          exit 1
        fi

        rm -f -- "$bench_output_path" "$pidstat_output_path"

        if [[ "$profiling_enabled" == "1" ]]; then
          restore_output_owner_if_sudo_user "$perf_data_path"
        fi
        if [[ "$sample_bpf_enabled" == "1" && -n "$bpf_samples_path" ]]; then
          restore_output_owner_if_sudo_user "$bpf_samples_path" "$bpf_sampler_log_path"
          if [[ ! -s "$bpf_samples_path" ]]; then
            echo "BPF sampler produced no data for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
            if [[ -n "$bpf_sampler_log_path" && -s "$bpf_sampler_log_path" ]]; then
              cat "$bpf_sampler_log_path" >&2
            fi
            exit 1
          fi
        fi

        if [[ -z "$throughput" || -z "$elapsed_seconds" || -z "$total_operations" || -z "$avg_lock_hold_ns" || -z "$avg_wait_ns_estimated" || -z "$avg_lock_handoff_ns_estimated" || -z "$lock_hold_samples" || -z "$avg_cpu_pct" ]]; then
          echo "Failed to parse benchmark output for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
          echo "$bench_output" >&2
          exit 1
        fi

        raw_row=(
          "$t" "$c" "$o" "$r"
          "$throughput" "$elapsed_seconds" "$total_operations"
          "$avg_lock_hold_ns" "$avg_wait_ns_estimated" "$avg_lock_handoff_ns_estimated" "$lock_hold_samples" "$avg_cpu_pct"
        )
        if [[ "$profiling_enabled" == "1" ]]; then
          raw_row+=("$perf_data_path")
        fi
        if [[ "$sample_bpf_enabled" == "1" ]]; then
          raw_row+=("$bpf_samples_path" "$sample_bpf_layout" "$sample_bpf_interval_us")
        fi
        (
          IFS=,
          printf "%s\n" "${raw_row[*]}"
        ) >> "$output_raw"
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

    if ($6 != "")  { sum_elapsed[key] += ($6 + 0.0); cnt_elapsed[key]++ }
    if ($7 != "")  { sum_total_ops[key] += ($7 + 0.0); cnt_total_ops[key]++ }
    if ($8 != "")  { sum_lock_hold_ns[key] += ($8 + 0.0); cnt_lock_hold_ns[key]++ }
    if ($9 != "")  { sum_wait_ns[key] += ($9 + 0.0); cnt_wait_ns[key]++ }
    if ($10 != "") { sum_handoff_ns[key] += ($10 + 0.0); cnt_handoff_ns[key]++ }
    if ($11 != "") { sum_lock_hold_samples[key] += ($11 + 0.0); cnt_lock_hold_samples[key]++ }
    if ($12 != "") { sum_cpu_pct[key] += ($12 + 0.0); cnt_cpu_pct[key]++ }
  }
  END {
    print "threads,critical_iters,outside_iters,repeats,mean_throughput_ops_per_sec,elapsed_seconds,total_operations,avg_lock_hold_ns,avg_wait_ns_estimated,avg_lock_handoff_ns_estimated,lock_hold_samples,avg_cpu_pct"
    for (k in n) {
      mean = sum[k] / n[k]

      mean_elapsed = (cnt_elapsed[k] > 0) ? (sum_elapsed[k] / cnt_elapsed[k]) : 0
      mean_total_ops = (cnt_total_ops[k] > 0) ? (sum_total_ops[k] / cnt_total_ops[k]) : 0
      mean_lock_hold = (cnt_lock_hold_ns[k] > 0) ? (sum_lock_hold_ns[k] / cnt_lock_hold_ns[k]) : 0
      mean_wait_ns = (cnt_wait_ns[k] > 0) ? (sum_wait_ns[k] / cnt_wait_ns[k]) : 0
      mean_handoff_ns = (cnt_handoff_ns[k] > 0) ? (sum_handoff_ns[k] / cnt_handoff_ns[k]) : 0
      mean_lock_hold_samples = (cnt_lock_hold_samples[k] > 0) ? (sum_lock_hold_samples[k] / cnt_lock_hold_samples[k]) : 0
      mean_cpu_pct = (cnt_cpu_pct[k] > 0) ? (sum_cpu_pct[k] / cnt_cpu_pct[k]) : 0

      split(k, parts, FS)
      printf "%s,%s,%s,%d,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f\n", \
             parts[1], parts[2], parts[3], n[k], mean, mean_elapsed, \
             mean_total_ops, mean_lock_hold, mean_wait_ns, mean_handoff_ns, \
             mean_lock_hold_samples, mean_cpu_pct
    }
  }
' "$output_raw" | sort -t',' -k1,1n -k2,2n -k3,3n > "$output_summary"

restore_output_owner_if_sudo_user "$output_raw" "$output_summary"

echo "Raw results: $output_raw" >&2
echo "Summary results: $output_summary" >&2
