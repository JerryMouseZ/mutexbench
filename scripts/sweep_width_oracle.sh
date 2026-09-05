#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd -- "${MUTEXBENCH_DIR}/../.." && pwd)"

usage() {
  cat <<'EOF'
Drive sweep_mutex_throughput.sh across per-class width settings and a (critical-ns,
outside-ns) grid, then merge every per-run summary.csv into one combined CSV.

One sweep_mutex_throughput.sh invocation is issued per (arm, width, critical-ns,
outside-ns); each invocation covers the whole thread list internally. Results land in
  <output-root>/<arm>[_w<width>]_c<critical>_o<outside>/{raw,summary}.csv
and are merged into <output-root>/combined.csv, keyed so that the per-point argmax over
width is a group-by on (threads, critical_ns, outside_ns).

Arms:
  fixed      ACCORDIN_WIDTH_CONTROL=1 plus ACCORDIN_FIXED_WIDTH=<width>, one run per width
  adaptive   ACCORDIN_WIDTH_CONTROL=1 with no fixed width  (--with-adaptive)
  gate_only  no width variables at all                     (--with-gate-only)

Operating point: width limiting can only pay off when CPUs are scarce relative to the
number of runnable lock waiters. On an under-subscribed machine throughput is monotone
in width and no interior optimum exists. Keep threads well above the number of CPUs the
benchmark may use, either by raising --threads or by shrinking the CPU budget with
--cpu-list / --cpu-count (both wrap the sweep in taskset, whose affinity the benchmark
inherits).

The Accordin scheduler needs root: run this script under sudo, e.g.
  sudo env PATH="$PATH" scripts/sweep_width_oracle.sh --output-root results/width_oracle

Usage:
  scripts/sweep_width_oracle.sh [options]

Options:
  --sweep-script PATH        Sweep script (default: <scripts>/sweep_mutex_throughput.sh)
  --binary PATH              Benchmark binary forwarded to the sweep script
  --lock-kind K              Lock kind (default: mcs_tas_accordin_direct)
  --direct-lib PATH          Value for MCS_TAS_ACCORDIN_DIRECT_LIB
                             (default: <repo>/target/release/libmcs_tas_accordin_direct.so;
                             pass "none" to leave it unset)
  --bench-ld-preload PATH    Forwarded to the sweep script unchanged
  --bench-env KEY=VALUE      Extra benchmark-only variable added to every arm; repeatable
  --widths CSV               Fixed widths to scan (default: 1,2,3,4,6,8,12,16,24,32)
  --threads CSV              Thread counts per sweep (default: 32,96,192)
  --grid CSV                 Critical:outside pairs in ns
                             (default: 300:3000,1000:10000,3000:300)
  --critical-ns CSV          Critical-section ns; combined with --outside-ns as a full
                             cross product, replacing --grid
  --outside-ns CSV           Non-critical-section ns; see --critical-ns
  --duration-ms N            Measurement duration in ms (default: 5000)
  --warmup-duration-ms N     Warmup duration in ms (default: 1000)
  --repeats N                Runs per parameter point (default: 4)
  --cpu-list LIST            Restrict the CPU budget with taskset -c LIST
  --cpu-count N              Restrict the CPU budget to CPUs 0-(N-1)
  --with-adaptive            Also run the adaptive arm at every grid point
  --with-gate-only           Also run the gate-only arm at every grid point
  --output-root DIR          Output root (default: <mutexbench>/results/width_oracle)
  --force                    Reuse a non-empty output root
  --dry-run                  Print the sweep commands without running them
  -h, --help                 Show this help

Example:
  sudo env PATH="$PATH" scripts/sweep_width_oracle.sh \
    --threads 192 \
    --cpu-count 16 \
    --widths 1,2,4,8,16 \
    --grid 300:3000,1000:10000 \
    --with-adaptive --with-gate-only \
    --output-root results/width_oracle_cpu16
EOF
}

sweep_script="$SCRIPT_DIR/sweep_mutex_throughput.sh"
binary=""
lock_kind="mcs_tas_accordin_direct"
direct_lib="$PROJECT_ROOT/target/release/libmcs_tas_accordin_direct.so"
bench_ld_preload=""
declare -a extra_bench_env=()
widths_csv="1,2,3,4,6,8,12,16,24,32"
threads_csv="32,96,192"
grid_csv="300:3000,1000:10000,3000:300"
critical_ns_csv=""
outside_ns_csv=""
duration_ms="5000"
warmup_duration_ms="1000"
repeats="4"
cpu_list=""
cpu_count=""
with_adaptive="0"
with_gate_only="0"
output_root="$MUTEXBENCH_DIR/results/width_oracle"
force="0"
dry_run="0"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sweep-script)
      sweep_script="${2:-}"
      shift 2
      ;;
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --lock-kind)
      lock_kind="${2:-}"
      shift 2
      ;;
    --direct-lib)
      direct_lib="${2:-}"
      shift 2
      ;;
    --bench-ld-preload)
      bench_ld_preload="${2:-}"
      shift 2
      ;;
    --bench-env)
      extra_bench_env+=("${2:-}")
      shift 2
      ;;
    --widths)
      widths_csv="${2:-}"
      shift 2
      ;;
    --threads)
      threads_csv="${2:-}"
      shift 2
      ;;
    --grid)
      grid_csv="${2:-}"
      shift 2
      ;;
    --critical-ns)
      critical_ns_csv="${2:-}"
      shift 2
      ;;
    --outside-ns)
      outside_ns_csv="${2:-}"
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
    --repeats)
      repeats="${2:-}"
      shift 2
      ;;
    --cpu-list)
      cpu_list="${2:-}"
      shift 2
      ;;
    --cpu-count)
      cpu_count="${2:-}"
      shift 2
      ;;
    --with-adaptive)
      with_adaptive="1"
      shift
      ;;
    --with-gate-only)
      with_gate_only="1"
      shift
      ;;
    --output-root)
      output_root="${2:-}"
      shift 2
      ;;
    --force)
      force="1"
      shift
      ;;
    --dry-run)
      dry_run="1"
      shift
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

is_env_assignment() {
  local value="$1"
  [[ "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]]
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

parse_csv_uints() {
  local csv="$1"
  local value_name="$2"
  local -n output_array="$3"

  IFS=',' read -r -a output_array <<< "$csv"
  if [[ ${#output_array[@]} -eq 0 ]]; then
    echo "No values in ${value_name}" >&2
    exit 1
  fi
  for i in "${!output_array[@]}"; do
    local value="${output_array[$i]//[[:space:]]/}"
    if ! is_uint "$value" || [[ "$value" -eq 0 ]]; then
      echo "${value_name} requires integers > 0, got: ${output_array[$i]}" >&2
      exit 1
    fi
    output_array[$i]="$value"
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
if ! is_uint "$repeats" || [[ "$repeats" -eq 0 ]]; then
  echo "--repeats must be an integer > 0" >&2
  exit 1
fi
if [[ -n "$cpu_list" && -n "$cpu_count" ]]; then
  echo "--cpu-list and --cpu-count are mutually exclusive" >&2
  exit 1
fi
if [[ -n "$cpu_count" ]]; then
  if ! is_uint "$cpu_count" || [[ "$cpu_count" -eq 0 ]]; then
    echo "--cpu-count must be an integer > 0" >&2
    exit 1
  fi
  cpu_list="0-$((cpu_count - 1))"
fi
if [[ -n "$cpu_list" && ! "$cpu_list" =~ ^[0-9]+([-,][0-9]+)*$ ]]; then
  echo "--cpu-list must be a taskset CPU list such as 0-15 or 0,2,4" >&2
  exit 1
fi
for env_entry in "${extra_bench_env[@]}"; do
  if ! is_env_assignment "$env_entry"; then
    echo "--bench-env must be KEY=VALUE with KEY matching [A-Za-z_][A-Za-z0-9_]*, got: $env_entry" >&2
    exit 1
  fi
done
if [[ -n "$critical_ns_csv" || -n "$outside_ns_csv" ]]; then
  if [[ -z "$critical_ns_csv" || -z "$outside_ns_csv" ]]; then
    echo "--critical-ns and --outside-ns must be given together" >&2
    exit 1
  fi
fi

declare -a widths=()
declare -a grid_points=()
parse_csv_uints "$widths_csv" "--widths" widths

if [[ -n "$critical_ns_csv" ]]; then
  declare -a critical_values=()
  declare -a outside_values=()
  parse_csv_uints "$critical_ns_csv" "--critical-ns" critical_values
  parse_csv_uints "$outside_ns_csv" "--outside-ns" outside_values
  for critical_value in "${critical_values[@]}"; do
    for outside_value in "${outside_values[@]}"; do
      grid_points+=("${critical_value}:${outside_value}")
    done
  done
else
  IFS=',' read -r -a grid_points <<< "$grid_csv"
  if [[ ${#grid_points[@]} -eq 0 ]]; then
    echo "No values in --grid" >&2
    exit 1
  fi
  for i in "${!grid_points[@]}"; do
    point="${grid_points[$i]//[[:space:]]/}"
    if [[ ! "$point" =~ ^[0-9]+:[0-9]+$ ]]; then
      echo "--grid entries must be CRITICAL_NS:OUTSIDE_NS, got: ${grid_points[$i]}" >&2
      exit 1
    fi
    grid_points[$i]="$point"
  done
fi

# The thread list is forwarded verbatim; validate and normalize it here so a bad list
# fails before any sweep starts.
declare -a threads=()
parse_csv_uints "$threads_csv" "--threads" threads
threads_csv="$(IFS=','; printf "%s" "${threads[*]}")"

sweep_script="$(resolve_executable_path "$sweep_script" "$MUTEXBENCH_DIR")"
if [[ ! -x "$sweep_script" ]]; then
  echo "Sweep script is not executable: $sweep_script" >&2
  exit 1
fi
if [[ -n "$cpu_list" ]] && ! command -v taskset >/dev/null 2>&1; then
  echo "taskset not found in PATH" >&2
  exit 1
fi
if [[ "$direct_lib" != "none" ]]; then
  direct_lib="$(resolve_output_path "$direct_lib" "$PROJECT_ROOT")"
  if [[ "$dry_run" != "1" && ! -f "$direct_lib" ]]; then
    echo "--direct-lib file not found: $direct_lib" >&2
    exit 1
  fi
fi
if [[ "$dry_run" != "1" && "$EUID" -ne 0 ]]; then
  echo "Warning: not running as root; Accordin lock kinds will fail to load the scheduler." >&2
fi

output_root="$(resolve_output_path "$output_root" "$MUTEXBENCH_DIR")"
combined_csv="$output_root/combined.csv"
if [[ "$dry_run" != "1" ]]; then
  if [[ -e "$combined_csv" && "$force" != "1" ]]; then
    echo "Output root already contains combined.csv; pass --force to reuse: $output_root" >&2
    exit 1
  fi
  mkdir -p "$output_root"
fi

declare -a arms=()
for width in "${widths[@]}"; do
  arms+=("fixed:${width}")
done
if [[ "$with_adaptive" == "1" ]]; then
  arms+=("adaptive:")
fi
if [[ "$with_gate_only" == "1" ]]; then
  arms+=("gate_only:")
fi

run_dir_name() {
  local arm="$1"
  local width="$2"
  local critical_ns="$3"
  local outside_ns="$4"

  if [[ "$arm" == "fixed" ]]; then
    printf "fixed_w%s_c%s_o%s\n" "$width" "$critical_ns" "$outside_ns"
  else
    printf "%s_c%s_o%s\n" "$arm" "$critical_ns" "$outside_ns"
  fi
}

total_runs=$(( ${#arms[@]} * ${#grid_points[@]} ))
current_run=0
declare -a completed_dirs=()
declare -a completed_arms=()
declare -a completed_widths=()

for arm_spec in "${arms[@]}"; do
  arm="${arm_spec%%:*}"
  width="${arm_spec#*:}"
  for grid_point in "${grid_points[@]}"; do
    critical_ns="${grid_point%%:*}"
    outside_ns="${grid_point##*:}"
    current_run=$((current_run + 1))

    run_dir="$output_root/$(run_dir_name "$arm" "$width" "$critical_ns" "$outside_ns")"

    declare -a bench_env_args=()
    if [[ "$direct_lib" != "none" ]]; then
      bench_env_args+=("MCS_TAS_ACCORDIN_DIRECT_LIB=$direct_lib")
    fi
    case "$arm" in
      fixed)
        bench_env_args+=("ACCORDIN_WIDTH_CONTROL=1" "ACCORDIN_FIXED_WIDTH=$width")
        ;;
      adaptive)
        bench_env_args+=("ACCORDIN_WIDTH_CONTROL=1")
        ;;
    esac
    if [[ ${#extra_bench_env[@]} -gt 0 ]]; then
      bench_env_args+=("${extra_bench_env[@]}")
    fi

    cmd=()
    if [[ -n "$cpu_list" ]]; then
      cmd+=(taskset -c "$cpu_list")
    fi
    cmd+=(
      "$sweep_script"
      --lock-kind "$lock_kind"
      --threads "$threads_csv"
      --critical-ns "$critical_ns"
      --outside-ns "$outside_ns"
      --duration-ms "$duration_ms"
      --warmup-duration-ms "$warmup_duration_ms"
      --repeats "$repeats"
      --output-root "$run_dir"
    )
    if [[ -n "$binary" ]]; then
      cmd+=(--binary "$binary")
    fi
    if [[ -n "$bench_ld_preload" ]]; then
      cmd+=(--bench-ld-preload "$bench_ld_preload")
    fi
    for env_entry in "${bench_env_args[@]}"; do
      cmd+=(--bench-env "$env_entry")
    done

    echo "[${current_run}/${total_runs}] arm=${arm} width=${width:-n/a} critical=${critical_ns} outside=${outside_ns} cpu_list=${cpu_list:-all}" >&2
    if [[ "$dry_run" == "1" ]]; then
      printf "%q " "${cmd[@]}"
      printf "\n"
      continue
    fi

    mkdir -p "$run_dir"
    "${cmd[@]}"
    completed_dirs+=("$run_dir")
    completed_arms+=("$arm")
    completed_widths+=("$width")
  done
done

if [[ "$dry_run" == "1" ]]; then
  echo "Combined CSV would be written to: $combined_csv" >&2
  exit 0
fi

combined_header="arm,width,cpu_list,threads,critical_ns,outside_ns,repeats,mean_throughput_ops_per_sec,elapsed_seconds,total_operations,avg_lock_hold_ns,avg_wait_ns_estimated,avg_lock_handoff_ns_estimated,lock_hold_samples,avg_cpu_pct,output_dir"
printf "%s\n" "$combined_header" > "$combined_csv"

for index in "${!completed_dirs[@]}"; do
  run_dir="${completed_dirs[$index]}"
  arm="${completed_arms[$index]}"
  width="${completed_widths[$index]}"
  summary_csv="$run_dir/summary.csv"
  if [[ ! -s "$summary_csv" ]]; then
    echo "Missing summary CSV, skipping in combined output: $summary_csv" >&2
    continue
  fi
  awk -F',' -v arm="$arm" -v width="$width" -v cpu_list="${cpu_list:-all}" -v run_dir="$run_dir" '
    NR == 1 { next }
    NF > 0 {
      printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
             arm, (width == "" ? "n/a" : width), cpu_list,
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, run_dir
    }
  ' "$summary_csv" >> "$combined_csv"
done

if [[ "$EUID" -eq 0 && -n "${SUDO_UID:-}" && -n "${SUDO_GID:-}" ]]; then
  chown "$SUDO_UID:$SUDO_GID" "$combined_csv" "$output_root" || true
fi

echo "Combined results: $combined_csv" >&2
