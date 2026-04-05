#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd -- "${MUTEXBENCH_DIR}/../.." && pwd)"

usage() {
  cat <<'EOF'
Run mutex_bench under perf and report how much sampled user time lands in McsTasLockRaw lock code.

Usage:
  scripts/report_mcs_tas_raw_lock_pct.sh [options]

Options:
  --binary PATH                Benchmark binary path (default: <mutexbench>/mutex_bench)
  --lib PATH                   Prebuilt libmcs_tas_simple.so path
                               (default: <repo>/target/release/libmcs_tas_simple.so)
  --threads CSV                Thread counts, comma-separated (default: 32)
  --critical-ns CSV            Critical-section burn time in ns (default: 350)
  --outside-ns CSV             Non-critical-section burn time in ns (default: 350)
  --critical-iters CSV         Legacy alias for --critical-ns
  --duration-ms N              Measurement duration in ms (default: 3000)
  --warmup-duration-ms N       Warmup duration in ms (default: 1000)
  --repeats N                  Runs per parameter point (default: 3)
  --perf-freq N                perf sampling frequency (default: 499)
  --sudo-mode MODE             MODE in {auto,always,none} (default: auto)
  --disable-bpf                Set MCS_TAS_SIMPLE_DISABLE_BPF=1 for the benchmark
  --stats-only                 Set MCS_TAS_SIMPLE_STATS_ONLY=1 for the benchmark
  --output-raw PATH            Raw per-run CSV
                               (default: <mutexbench>/results/mcs_tas_raw_perf/raw.csv)
  --output-summary PATH        Aggregated CSV
                               (default: <mutexbench>/results/mcs_tas_raw_perf/summary.csv)
  -h, --help                   Show this help

The script builds mcs_tas_simple with:
  cargo build -p mcs_tas_simple --release --features perf-symbols

It counts samples whose symbol matches one of:
  McsTasLockRaw::lock_slow
  McsTasLockRaw::try_lock_fast
  McsTasLockRaw::unlock_fast
  <McsTasLockRaw as LockBackend>::lock
  <McsTasLockRaw as LockBackend>::try_lock
  <McsTasLockRaw as LockBackend>::unlock
EOF
}

binary="$MUTEXBENCH_DIR/mutex_bench"
lib_path="$PROJECT_ROOT/target/release/libmcs_tas_simple.so"
threads_csv="32"
critical_iters_csv="350"
outside_iters_csv="350"
duration_ms="3000"
warmup_duration_ms="1000"
repeats="3"
perf_freq="499"
sudo_mode="auto"
disable_bpf="0"
stats_only="0"
lib_was_explicit="0"
output_raw="$MUTEXBENCH_DIR/results/mcs_tas_raw_perf/raw.csv"
output_summary="$MUTEXBENCH_DIR/results/mcs_tas_raw_perf/summary.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --binary)
      binary="${2:-}"
      shift 2
      ;;
    --lib)
      lib_path="${2:-}"
      lib_was_explicit="1"
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
    --repeats)
      repeats="${2:-}"
      shift 2
      ;;
    --perf-freq)
      perf_freq="${2:-}"
      shift 2
      ;;
    --sudo-mode)
      sudo_mode="${2:-}"
      shift 2
      ;;
    --disable-bpf)
      disable_bpf="1"
      shift
      ;;
    --stats-only)
      stats_only="1"
      shift
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

resolve_file_path() {
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

run_perf_command() {
  if [[ "$USE_SUDO" == "1" ]]; then
    sudo "$@"
  else
    "$@"
  fi
}

restore_file_owner_if_sudo_user() {
  local path="$1"
  local sudo_uid="${SUDO_UID:-}"
  local sudo_gid="${SUDO_GID:-}"
  local parent=""

  if [[ "$EUID" -ne 0 || -z "$sudo_uid" || -z "$sudo_gid" || -z "$path" ]]; then
    return 0
  fi

  if [[ -e "$path" ]]; then
    chown "$sudo_uid:$sudo_gid" "$path" >/dev/null 2>&1 || true
    chmod u+rw "$path" >/dev/null 2>&1 || true
  fi

  parent="$(dirname "$path")"
  if [[ -d "$parent" ]]; then
    chown "$sudo_uid:$sudo_gid" "$parent" >/dev/null 2>&1 || true
    chmod u+rwx "$parent" >/dev/null 2>&1 || true
  fi
}

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found in PATH: $cmd" >&2
    exit 1
  fi
}

if [[ "$disable_bpf" == "1" && "$stats_only" == "1" ]]; then
  echo "--disable-bpf and --stats-only are mutually exclusive" >&2
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
if ! is_uint "$repeats" || [[ "$repeats" -eq 0 ]]; then
  echo "--repeats must be an integer > 0" >&2
  exit 1
fi
if ! is_uint "$perf_freq" || [[ "$perf_freq" -eq 0 ]]; then
  echo "--perf-freq must be an integer > 0" >&2
  exit 1
fi
case "$sudo_mode" in
  auto|always|none)
    ;;
  *)
    echo "--sudo-mode must be one of: auto, always, none" >&2
    exit 1
    ;;
esac

declare -a threads=()
declare -a critical_iters=()
declare -a outside_iters=()
parse_csv_values "$threads_csv" "--threads" "no" threads
parse_csv_values "$critical_iters_csv" "--critical-ns" "yes" critical_iters
parse_csv_values "$outside_iters_csv" "--outside-ns" "yes" outside_iters

binary="$(resolve_executable_path "$binary" "$MUTEXBENCH_DIR")"
lib_path="$(resolve_file_path "$lib_path" "$PROJECT_ROOT")"
output_raw="$(resolve_output_path "$output_raw" "$MUTEXBENCH_DIR")"
output_summary="$(resolve_output_path "$output_summary" "$MUTEXBENCH_DIR")"

mkdir -p "$(dirname "$output_raw")"
mkdir -p "$(dirname "$output_summary")"

require_command perf
require_command python3
require_command nm
require_command cargo
require_command make

USE_SUDO="0"
case "$sudo_mode" in
  auto)
    if [[ "$EUID" -ne 0 ]]; then
      USE_SUDO="1"
    fi
    ;;
  always)
    USE_SUDO="1"
    ;;
  none)
    ;;
esac

if [[ ! -x "$binary" && "$(basename "$binary")" == "mutex_bench" ]]; then
  echo "Building mutex_bench..." >&2
  make -C "$MUTEXBENCH_DIR" mutex_bench >/dev/null
fi

if [[ ! -x "$binary" ]]; then
  echo "Benchmark binary is not executable: $binary" >&2
  exit 1
fi

if [[ "$lib_was_explicit" == "0" ]]; then
  echo "Building mcs_tas_simple with release profile..." >&2
  cargo build --manifest-path "$PROJECT_ROOT/Cargo.toml" -p mcs_tas_simple --release --features perf-symbols >/dev/null
fi

if [[ ! -f "$lib_path" ]]; then
  echo "Preload library not found: $lib_path" >&2
  exit 1
fi

nm_output="$(nm -C "$lib_path" 2>/dev/null || true)"
if ! grep -Eq 'McsTasLockRaw::(lock_slow|try_lock_fast|unlock_fast)|LockBackend>::(lock|try_lock|unlock)' <<< "$nm_output"; then
  echo "Expected McsTasLockRaw symbols not found in $lib_path" >&2
  echo "Rebuild with: cargo build -p mcs_tas_simple --release --features perf-symbols" >&2
  exit 1
fi

raw_output_dir="$(dirname "$output_raw")"
mkdir -p "$raw_output_dir"

printf "%s\n" \
  "threads,critical_iters,outside_iters,repeat,perf_total_samples,perf_lock_samples,perf_lock_pct,perf_data_path" \
  > "$output_raw"

raw_lock_symbol_regex='(McsTasLockRaw::(lock_slow|try_lock_fast|unlock_fast)|McsTasLockRaw as .*LockBackend>::(lock|try_lock|unlock))'
lib_name="$(basename "$lib_path")"

total_runs=$(( ${#threads[@]} * ${#critical_iters[@]} * ${#outside_iters[@]} * repeats ))
current_run=0

for t in "${threads[@]}"; do
  for c in "${critical_iters[@]}"; do
    for o in "${outside_iters[@]}"; do
      for ((r = 1; r <= repeats; ++r)); do
        current_run=$((current_run + 1))
        echo "[${current_run}/${total_runs}] threads=${t} critical=${c} outside=${o} repeat=${r}" >&2

        perf_basename="t${t}_c${c}_o${o}_r${r}.perf.data"
        perf_data_path="$raw_output_dir/$perf_basename"
        perf_script_path="$(mktemp)"
        trap 'rm -f -- "$perf_script_path"' EXIT

        bench_env=(env "LD_PRELOAD=$lib_path")
        if [[ "$disable_bpf" == "1" ]]; then
          bench_env+=("MCS_TAS_SIMPLE_DISABLE_BPF=1")
        fi
        if [[ "$stats_only" == "1" ]]; then
          bench_env+=("MCS_TAS_SIMPLE_STATS_ONLY=1")
        fi

        bench_cmd=(
          "$binary"
          --threads "$t"
          --lock-kind mutex
          --duration-ms "$duration_ms"
          --warmup-duration-ms "$warmup_duration_ms"
          --critical-ns "$c"
          --outside-ns "$o"
        )

        run_perf_command \
          perf record -q -F "$perf_freq" -e cpu-clock -o "$perf_data_path" -- \
          "${bench_env[@]}" "${bench_cmd[@]}"

        restore_file_owner_if_sudo_user "$perf_data_path"

        run_perf_command \
          perf script --demangle -F ip,sym,dso -i "$perf_data_path" > "$perf_script_path"

        perf_stats="$({
          awk \
            -v lib_name="$lib_name" \
            -v raw_lock_symbol_regex="$raw_lock_symbol_regex" '
              /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
              NF < 3 { next }
              {
                dso = $NF
                gsub(/^\(/, "", dso)
                gsub(/\)$/, "", dso)
                $1 = ""
                $NF = ""
                sub(/^[[:space:]]+/, "", $0)
                sub(/[[:space:]]+$/, "", $0)
                sym = $0
                total_samples += 1
                if (dso ~ ("/" lib_name "$") || dso == lib_name) {
                  lib_samples += 1
                  if (sym ~ raw_lock_symbol_regex) {
                    lock_samples += 1
                  }
                }
              }
              END {
                if (total_samples == 0) {
                  exit 1
                }
                printf "%d,%d,%d,%.6f\n", total_samples + 0, lib_samples + 0, lock_samples + 0, ((lock_samples + 0) * 100.0) / (total_samples + 0)
              }
            ' "$perf_script_path"
        } )" || {
          echo "Failed to parse perf samples for threads=${t} critical=${c} outside=${o} repeat=${r}" >&2
          exit 1
        }

        IFS=',' read -r perf_total_samples perf_lib_samples perf_lock_samples perf_lock_pct <<< "$perf_stats"

        if [[ "$perf_lib_samples" -eq 0 ]]; then
          echo "No samples attributed to $lib_name; perf symbolization is likely not working" >&2
          exit 1
        fi
        if [[ "$perf_lock_samples" -eq 0 ]]; then
          echo "No McsTasLockRaw samples found in perf output; rebuild with --release --features perf-symbols or increase duration" >&2
          exit 1
        fi

        printf "%s,%s,%s,%s,%s,%s,%s,%s\n" \
          "$t" "$c" "$o" "$r" "$perf_total_samples" "$perf_lock_samples" "$perf_lock_pct" "$perf_data_path" \
          >> "$output_raw"

        printf "perf_lock_pct=%.6f%% total_samples=%s lock_samples=%s\n" \
          "$perf_lock_pct" "$perf_total_samples" "$perf_lock_samples"

        rm -f -- "$perf_script_path"
        trap - EXIT
      done
    done
  done
done

python3 - "$output_raw" "$output_summary" <<'PY'
import csv
import sys
from collections import defaultdict

raw_path, summary_path = sys.argv[1:3]

with open(raw_path, newline="") as f:
    rows = list(csv.DictReader(f))

if not rows:
    raise SystemExit("raw.csv is empty")

grouped = defaultdict(lambda: {"count": 0, "total": 0.0, "lock": 0.0, "pct": 0.0})
for row in rows:
    key = (row["threads"], row["critical_iters"], row["outside_iters"])
    agg = grouped[key]
    agg["count"] += 1
    agg["total"] += float(row["perf_total_samples"])
    agg["lock"] += float(row["perf_lock_samples"])
    agg["pct"] += float(row["perf_lock_pct"])

with open(summary_path, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "threads",
            "critical_iters",
            "outside_iters",
            "repeats",
            "mean_perf_total_samples",
            "mean_perf_lock_samples",
            "mean_perf_lock_pct",
        ],
    )
    writer.writeheader()
    for threads, critical, outside in sorted(grouped, key=lambda item: tuple(map(int, item))):
        agg = grouped[(threads, critical, outside)]
        count = agg["count"]
        writer.writerow(
            {
                "threads": threads,
                "critical_iters": critical,
                "outside_iters": outside,
                "repeats": count,
                "mean_perf_total_samples": f"{agg['total'] / count:.6f}",
                "mean_perf_lock_samples": f"{agg['lock'] / count:.6f}",
                "mean_perf_lock_pct": f"{agg['pct'] / count:.6f}",
            }
        )
PY

echo "Raw results: $output_raw" >&2
echo "Summary results: $output_summary" >&2
