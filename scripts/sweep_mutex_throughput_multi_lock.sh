#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd -- "${MUTEXBENCH_DIR}/../.." && pwd)"
LITL_DIR="${LITL_DIR:-$PROJECT_ROOT/third_party/litl}"
MCS_ACCORDIN_DEBUG_COUNTERS="${MCS_ACCORDIN_DEBUG_COUNTERS:-}"
MCS_TAS_ACCORDIN_DEBUG_COUNTERS="${MCS_TAS_ACCORDIN_DEBUG_COUNTERS:-}"
MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS="${MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-$MCS_ACCORDIN_DEBUG_COUNTERS}"
MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS="${MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-$MCS_TAS_ACCORDIN_DEBUG_COUNTERS}"
TTAS_ACCORDIN_DEBUG_COUNTERS="${TTAS_ACCORDIN_DEBUG_COUNTERS:-}"
RECIPROCATING_ACCORDIN_DEBUG_COUNTERS="${RECIPROCATING_ACCORDIN_DEBUG_COUNTERS:-}"
if [[ -n "${FLEXGUARD_DIR:-}" ]]; then
  FLEXGUARD_DIR="$(cd -- "$FLEXGUARD_DIR" && pwd)"
elif [[ -d "${MUTEXBENCH_DIR}/../flexguard" ]]; then
  FLEXGUARD_DIR="$(cd -- "${MUTEXBENCH_DIR}/../flexguard" && pwd)"
else
  FLEXGUARD_DIR=""
fi

usage() {
  cat <<'EOF'
Run mutex throughput sweep for multiple lock modes (native built-in locks + interpose scripts).

Usage:
  scripts/sweep_mutex_throughput_multi_lock.sh --locks CSV [options] [sweep args...]

Options:
  --locks CSV                Required. Comma-separated lock scripts.
                             Item format:
                               1) builtin lock kind (native run): mutex|pthread_spinlock
                               2) native-mutex (alias of native:mutex)
                               3) native:<kind> where <kind> is mutex|pthread_spinlock
                               4) litl:<name>, e.g. litl:mbmcs_original, litl:mcs_spinlock;
                                  runs the benchmark under $LITL_DIR/lib<name>.sh so that
                                  algorithm is interposed on pthread_mutex_*. A bare name
                                  also resolves with an _original suffix
                               5) /path/to/interpose_mcs.sh
                               6) mcs=/path/to/interpose_custom.sh
                               7) non-builtin short name (e.g. flexguard),
                                  resolved as $FLEXGUARD_DIR/build/interpose_<name>.sh
                               8) mcs_accordin (litl:mcsaccordin_original plus the accordin env)
                               9) mcs_accordin_no_bpf (same as mcs_accordin with MCS_ACCORDIN_DIRECT_DISABLE_BPF=1)
                              10) mcs_tse (run benchmark with LD_PRELOAD=libmcs_tse.so)
                              11) mcs_tas_accordin (litl:mcstasaccordin_original plus the accordin env)
                              12) mcs_tas_accordin_no_bpf (same as mcs_tas_accordin with MCS_TAS_ACCORDIN_DIRECT_DISABLE_BPF=1)
                              13) ttas_accordin (run benchmark with LD_PRELOAD=libttas_accordin.so)
                              14) ttas_accordin_no_bpf (same as ttas_accordin with TTAS_ACCORDIN_DISABLE_BPF=1)
                              15) reciprocating_accordin (run benchmark with LD_PRELOAD=libreciprocating_accordin.so)
                              16) reciprocating_accordin_no_bpf (same as reciprocating_accordin with RECIPROCATING_ACCORDIN_DISABLE_BPF=1)
                             Name conflict rule:
                               - Builtin names always run as native locks.
                               - To run external lock with a builtin-like name,
                                 use explicit script path or name=/path/to/script.sh
                             For mcs_accordin direct library path:
                               1) use $MCS_ACCORDIN_DIRECT_LIB if set
                               2) else <repo>/target/<profile>/libmcs_accordin_direct.so
                               3) else <repo>/target/release/libmcs_accordin_direct.so
                               4) else <repo>/target/debug/libmcs_accordin_direct.so
                             For mcs_tas_accordin direct library path:
                               1) use $MCS_TAS_ACCORDIN_DIRECT_LIB if set
                               2) else <repo>/target/<profile>/libmcs_tas_accordin_direct.so
                               3) else <repo>/target/release/libmcs_tas_accordin_direct.so
                               4) else <repo>/target/debug/libmcs_tas_accordin_direct.so
                             For mcs_tse library path:
                               1) use $MCS_TSE_LIB if set
                               2) else <repo>/target/release/libmcs_tse.so
                               3) else <repo>/target/debug/libmcs_tse.so
                             For ttas_accordin library path:
                               1) use $TTAS_ACCORDIN_LIB if set
                               2) else <repo>/target/<profile>/libttas_accordin.so
                               3) else <repo>/target/release/libttas_accordin.so
                               4) else <repo>/target/debug/libttas_accordin.so
                             For reciprocating_accordin library path:
                               1) use $RECIPROCATING_ACCORDIN_LIB if set
                               2) else <repo>/target/<profile>/libreciprocating_accordin.so
                               3) else <repo>/target/release/libreciprocating_accordin.so
                               4) else <repo>/target/debug/libreciprocating_accordin.so
  --sweep-script PATH        Sweep script to run (default: <mutexbench>/scripts/sweep_mutex_throughput.sh)
  --output-root DIR          Output root directory (default: <mutexbench>/results)
  --profile                  Enable perf profiling, preserve perf.data beside raw.csv,
                             and generate perf_reports/*.report.txt plus *.script.txt
  --bpf-profile              Profile Accordin BPF scheduler programs with bpftool
                             while each BPF-backed lock sweep is running
  --bpf-profile-duration-s N Per-program bpftool profile duration in seconds (default: 2)
  --bpf-profile-programs CSV Comma-separated BPF program names to profile
                             (default: Accordin struct_ops/syscall programs)
  --sample-bpf               Record per-run accordin BPF sampler CSVs for BPF-backed accordin locks
  --sample-bpf-layout MODE   Sampler layout: auto|v1|v2|legacy|current (default: auto)
  --sample-bpf-interval-us N Sampler interval in microseconds (default: 500)
  --sudo-mode MODE           MODE in {all,auto,none} (default: all)
                             all: sudo for every lock run
                             auto: sudo only for flexguard*/hybridlock*/mcs_accordin*/mcs_tas_accordin*/ttas_accordin*/reciprocating_accordin* locks
                             none: never sudo
  --litl-dir DIR             LiTL checkout holding lib<name>.sh (default: <repo>/third_party/litl)
  --with-scx-lavd            Run scx_lavd in background for the whole sweep:
                               sudo /mnt/home/jz/scx/target/release/scx_lavd --per-cpu-dsq --performance
                             It will be stopped after all locks finish.
  --scx-lavd-bin PATH        scx_lavd binary path (default: /mnt/home/jz/scx/target/release/scx_lavd)
  --lb-accordin-sched-ext-conflict MODE
                             MODE in {stop,error,ignore} (default: stop)
                             How to handle active sched_ext before accordin locks:
                               stop: terminate current sched_ext owner process(es)
                               error: fail fast with owner diagnostics
                               ignore: run anyway (mcs_accordin/mcs_tas_accordin/ttas_accordin/reciprocating_accordin may fail to initialize)
  --dry-run                  Print commands only, do not execute
  -h, --help                 Show this help

All unknown args are forwarded to sweep script.
This includes repeatable --bench-env KEY=VALUE, which the sweep script applies only to
the benchmark binary (e.g. --bench-env ACCORDIN_WIDTH_CONTROL=1).
This script serializes globally with flock so concurrent invocations queue.
Default queue lock file: /tmp/mutexbench-sweep-multi-lock.lock
Override queue lock file with env: MUTEXBENCH_MULTI_LOCK_LOCK_FILE=/path/to/lock
Do not pass --output-raw / --output-summary; they are generated per lock:
  <output-root>/<lock>/raw.csv
  <output-root>/<lock>/summary.csv
With --profile, perf artifacts are generated per lock:
  <output-root>/<lock>/t<threads>_c<critical>_o<outside>_r<repeat>.perf.data
  <output-root>/<lock>/perf_reports/t<threads>_c<critical>_o<outside>_r<repeat>.report.txt
  <output-root>/<lock>/perf_reports/t<threads>_c<critical>_o<outside>_r<repeat>.script.txt
  <output-root>/<lock>/perf_reports/index.csv
With --bpf-profile, bpftool program-profile artifacts are generated per lock:
  <output-root>/<lock>/bpf_profiles/index.csv
  <output-root>/<lock>/bpf_profiles/summary.csv
  <output-root>/<lock>/bpf_profiles/sample<N>_<program>.profile.txt

Example:
  scripts/sweep_mutex_throughput_multi_lock.sh \
    --locks mutex,litl:mbmcs_original,litl:ticket_original \
    --sudo-mode all \
    --threads 1,2,4,8,16 \
    --critical-ns 10,100,500 \
    --outside-ns 10,100,500 \
    --duration-ms 1000 \
    --repeats 5 \
    --output-root results
EOF
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

trim_spaces() {
  local v="$1"
  v="${v#"${v%%[![:space:]]*}"}"
  v="${v%"${v##*[![:space:]]}"}"
  printf "%s\n" "$v"
}

is_positive_uint() {
  local value="$1"
  [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]]
}

split_csv_values() {
  local csv="$1"
  local -n out_ref="$2"
  local -a values=()
  local value=""

  out_ref=()
  IFS=',' read -r -a values <<< "$csv"
  for value in "${values[@]}"; do
    value="$(trim_spaces "$value")"
    [[ -z "$value" ]] && continue
    out_ref+=("$value")
  done
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
      elif [[ -x "$base_dir/$path" ]]; then
        printf "%s\n" "$base_dir/$path"
      elif [[ -n "${FLEXGUARD_DIR:-}" && -x "$FLEXGUARD_DIR/$path" ]]; then
        printf "%s\n" "$FLEXGUARD_DIR/$path"
      else
        printf "%s\n" "$path"
      fi
      ;;
  esac
}

resolve_preload_lib_path() {
  local env_var_name="$1"
  local default_release="$2"
  local default_debug="$3"
  local path="${!env_var_name:-}"

  if [[ -n "$path" ]]; then
    path="$(expand_home "$path")"
    case "$path" in
      /*)
        printf "%s\n" "$path"
        ;;
      *)
        if [[ -f "$path" ]]; then
          printf "%s\n" "$path"
        elif [[ -f "$PROJECT_ROOT/$path" ]]; then
          printf "%s\n" "$PROJECT_ROOT/$path"
        elif [[ -f "$MUTEXBENCH_DIR/$path" ]]; then
          printf "%s\n" "$MUTEXBENCH_DIR/$path"
        else
          printf "%s\n" "$path"
        fi
        ;;
    esac
    return 0
  fi

  if [[ -f "$default_release" ]]; then
    printf "%s\n" "$default_release"
    return 0
  fi
  if [[ -f "$default_debug" ]]; then
    printf "%s\n" "$default_debug"
    return 0
  fi

  printf "%s\n" "$default_release"
}

resolve_mcs_accordin_lib_path() {
  resolve_preload_lib_path \
    "MCS_ACCORDIN_DIRECT_LIB" \
    "$PROJECT_ROOT/target/release/libmcs_accordin_direct.so" \
    "$PROJECT_ROOT/target/debug/libmcs_accordin_direct.so"
}

resolve_mcs_tas_accordin_direct_lib_path() {
  resolve_preload_lib_path \
    "MCS_TAS_ACCORDIN_DIRECT_LIB" \
    "$PROJECT_ROOT/target/release/libmcs_tas_accordin_direct.so" \
    "$PROJECT_ROOT/target/debug/libmcs_tas_accordin_direct.so"
}

resolve_mcs_tse_lib_path() {
  resolve_preload_lib_path \
    "MCS_TSE_LIB" \
    "$PROJECT_ROOT/target/release/libmcs_tse.so" \
    "$PROJECT_ROOT/target/debug/libmcs_tse.so"
}

resolve_ttas_accordin_lib_path() {
  resolve_preload_lib_path \
    "TTAS_ACCORDIN_LIB" \
    "$PROJECT_ROOT/target/release/libttas_accordin.so" \
    "$PROJECT_ROOT/target/debug/libttas_accordin.so"
}

resolve_reciprocating_accordin_lib_path() {
  resolve_preload_lib_path \
    "RECIPROCATING_ACCORDIN_LIB" \
    "$PROJECT_ROOT/target/release/libreciprocating_accordin.so" \
    "$PROJECT_ROOT/target/debug/libreciprocating_accordin.so"
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
      chown "$sudo_uid:$sudo_gid" "$path" >/dev/null 2>&1 || true
      chmod u+rw "$path" >/dev/null 2>&1 || true
    fi

    parent="$(dirname "$path")"
    if [[ -d "$parent" ]]; then
      chown "$sudo_uid:$sudo_gid" "$parent" >/dev/null 2>&1 || true
      chmod u+rwx "$parent" >/dev/null 2>&1 || true
    fi
  done
}

resolve_flexguard_short_lock_script() {
  local short_name="$1"
  local candidate_script=""
  local build_target=""

  if [[ -z "${FLEXGUARD_DIR:-}" ]]; then
    return 1
  fi

  candidate_script="$FLEXGUARD_DIR/build/interpose_${short_name}.sh"
  if [[ -x "$candidate_script" ]]; then
    printf "%s\n" "$candidate_script"
    return 0
  fi

  if [[ ! -f "$FLEXGUARD_DIR/Makefile" ]]; then
    return 1
  fi

  build_target="build/interpose_${short_name}.sh"
  echo "Building flexguard helper: ${build_target}" >&2
  if ! make -C "$FLEXGUARD_DIR" "$build_target" >/dev/null; then
    echo "Failed to build flexguard helper: ${build_target}" >&2
    return 1
  fi
  if [[ ! -x "$candidate_script" ]]; then
    echo "Built flexguard helper is not executable: $candidate_script" >&2
    return 1
  fi

  printf "%s\n" "$candidate_script"
}

ensure_queue_lock_file() {
  local path="$1"
  local dir=""

  dir="$(dirname "$path")"
  mkdir -p "$dir"

  if [[ ! -e "$path" ]]; then
    (
      umask 000
      : > "$path"
    )
  fi
  chmod 0644 "$path" >/dev/null 2>&1 || true

  if [[ ! -r "$path" ]]; then
    echo "Queue lock file is not readable: $path" >&2
    echo "Fix permissions first, for example: sudo chmod 0644 '$path'" >&2
    return 1
  fi
}

acquire_queue_lock() {
  local path="$1"

  if ! command -v flock >/dev/null 2>&1; then
    echo "flock is required to serialize multi-lock sweeps, but it was not found in PATH." >&2
    return 1
  fi

  ensure_queue_lock_file "$path"

  exec {QUEUE_LOCK_FD}<"$path"
  if flock -n "$QUEUE_LOCK_FD"; then
    echo "[queue] Acquired global multi-lock sweep lock: $path" >&2
    return 0
  fi

  echo "[queue] Waiting for global multi-lock sweep lock: $path" >&2
  flock "$QUEUE_LOCK_FD"
  echo "[queue] Acquired global multi-lock sweep lock: $path" >&2
}

contains_flag() {
  local needle="$1"
  shift
  local x
  for x in "$@"; do
    case "$x" in
      "$needle"|"$needle"=*)
        return 0
        ;;
    esac
  done
  return 1
}

is_builtin_lock_kind() {
  local kind="$1"
  case "$kind" in
    mutex|pthread_spinlock|pthread-spinlock)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

resolve_litl_launcher() {
  local name="$1"
  local candidate=""

  case "$name" in
    */*)
      printf "%s\n" "$(resolve_executable_path "$name" "$MUTEXBENCH_DIR")"
      return 0
      ;;
  esac
  for candidate in "$litl_dir/lib${name}.sh" "$litl_dir/lib${name}_original.sh"; do
    if [[ -f "$candidate" ]]; then
      printf "%s\n" "$candidate"
      return 0
    fi
  done
  return 1
}

require_litl_launcher() {
  local name="$1"
  local resolved=""

  if ! resolved="$(resolve_litl_launcher "$name")" || [[ ! -f "$resolved" ]]; then
    echo "LiTL launcher not found for '$name' (looked under $litl_dir)" >&2
    echo "Build it first, e.g. make -C $litl_dir ALGORITHMS=\"${name}\" all" >&2
    exit 1
  fi
  printf "%s\n" "$resolved"
}

run_with_optional_sudo() {
  local use_sudo="$1"
  shift
  if [[ "$use_sudo" == "yes" && "$EUID" -ne 0 ]]; then
    sudo -- "$@"
  else
    "$@"
  fi
}

list_sched_ext_struct_ops_ids() {
  local use_sudo="$1"
  local out=""

  if ! out="$(run_with_optional_sudo "$use_sudo" bpftool struct_ops show 2>/dev/null || true)"; then
    return 0
  fi
  if [[ -z "$out" ]]; then
    return 0
  fi

  awk '$3 == "sched_ext_ops" { gsub(":", "", $1); print $1 }' <<< "$out"
}

list_sched_ext_owner_pids_by_map_id() {
  local use_sudo="$1"
  local map_id="$2"
  local out=""

  if ! out="$(run_with_optional_sudo "$use_sudo" bpftool map show id "$map_id" 2>/dev/null || true)"; then
    return 0
  fi
  if [[ -z "$out" ]]; then
    return 0
  fi

  grep -oE '\([0-9]+\)' <<< "$out" | tr -d '()' || true
}

format_sched_ext_owner_diag() {
  local use_sudo="$1"
  local -A seen=()
  local -a entries=()
  local id=""
  local pid=""
  local comm=""

  while IFS= read -r id; do
    [[ -z "$id" ]] && continue
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      [[ -n "${seen[$pid]:-}" ]] && continue
      seen["$pid"]=1

      comm=""
      comm="$(run_with_optional_sudo "$use_sudo" ps -p "$pid" -o comm= 2>/dev/null | awk '{$1=$1; print}' || true)"
      if [[ -n "$comm" ]]; then
        entries+=("${comm}(${pid})")
      else
        entries+=("pid(${pid})")
      fi
    done < <(list_sched_ext_owner_pids_by_map_id "$use_sudo" "$id")
  done < <(list_sched_ext_struct_ops_ids "$use_sudo")

  if [[ ${#entries[@]} -eq 0 ]]; then
    printf "%s\n" "unknown"
    return 0
  fi

  printf "%s\n" "${entries[*]}"
}

sched_ext_state() {
  local state_file="/sys/kernel/sched_ext/state"
  if [[ -r "$state_file" ]]; then
    cat "$state_file"
  fi
}

sched_ext_ops_name() {
  local ops_file="/sys/kernel/sched_ext/root/ops"
  if [[ -r "$ops_file" ]]; then
    cat "$ops_file"
  fi
}

ensure_mcs_tas_accordin_sched_ext_ready() {
  local use_sudo="$1"
  local conflict_mode="$2"
  local state=""
  local ops=""

  state="$(sched_ext_state || true)"
  if [[ "$state" != "enabled" ]]; then
    return 0
  fi

  ops="$(sched_ext_ops_name || true)"
  if [[ "$conflict_mode" == "ignore" ]]; then
    return 0
  fi

  if [[ "$conflict_mode" == "error" ]]; then
    local owners="unknown"
    owners="$(format_sched_ext_owner_diag "$use_sudo" || true)"
    echo "mcs_accordin/mcs_tas_accordin/ttas_accordin/reciprocating_accordin locks require exclusive sched_ext, but current state is enabled (ops=${ops:-unknown})." >&2
    echo "Current owner(s): ${owners}" >&2
    echo "Stop active scheduler first, or use --lb-accordin-sched-ext-conflict stop." >&2
    return 1
  fi

  if ! command -v bpftool >/dev/null 2>&1; then
    echo "sched_ext is enabled (ops=${ops:-unknown}), but bpftool is unavailable; cannot auto-stop owner process." >&2
    return 1
  fi
  if [[ "$use_sudo" != "yes" && "$EUID" -ne 0 ]]; then
    echo "sched_ext is enabled (ops=${ops:-unknown}) and stopping it requires root." >&2
    echo "Use --sudo-mode all/auto, run script under sudo, or switch to --lb-accordin-sched-ext-conflict error." >&2
    return 1
  fi

  local -A seen_pids=()
  local -a owner_pids=()
  local id=""
  local pid=""

  while IFS= read -r id; do
    [[ -z "$id" ]] && continue
    while IFS= read -r pid; do
      [[ -z "$pid" ]] && continue
      [[ -n "${seen_pids[$pid]:-}" ]] && continue
      if run_with_optional_sudo "$use_sudo" kill -0 "$pid" >/dev/null 2>&1; then
        seen_pids["$pid"]=1
        owner_pids+=("$pid")
      fi
    done < <(list_sched_ext_owner_pids_by_map_id "$use_sudo" "$id")
  done < <(list_sched_ext_struct_ops_ids "$use_sudo")

  if [[ ${#owner_pids[@]} -eq 0 ]]; then
    echo "sched_ext is enabled (ops=${ops:-unknown}) but no owner PID was found; cannot auto-stop safely." >&2
    echo "Use: sudo bpftool map show | grep struct_ops -A2" >&2
    return 1
  fi

  echo "[accordin_preload] Active sched_ext detected (ops=${ops:-unknown}); stopping owner PIDs: ${owner_pids[*]}" >&2
  for pid in "${owner_pids[@]}"; do
    run_with_optional_sudo "$use_sudo" kill -TERM "$pid" >/dev/null 2>&1 || true
  done

  local i=0
  for ((i = 0; i < 50; ++i)); do
    state="$(sched_ext_state || true)"
    if [[ "$state" != "enabled" ]]; then
      break
    fi
    sleep 0.1
  done

  if [[ "$state" == "enabled" ]]; then
    echo "[accordin_preload] sched_ext still enabled after SIGTERM; sending SIGKILL to: ${owner_pids[*]}" >&2
    for pid in "${owner_pids[@]}"; do
      run_with_optional_sudo "$use_sudo" kill -KILL "$pid" >/dev/null 2>&1 || true
    done

    for ((i = 0; i < 50; ++i)); do
      state="$(sched_ext_state || true)"
      if [[ "$state" != "enabled" ]]; then
        break
      fi
      sleep 0.1
    done
  fi

  state="$(sched_ext_state || true)"
  if [[ "$state" == "enabled" ]]; then
    echo "Failed to clear active sched_ext (ops=${ops:-unknown}); mcs_accordin/mcs_tas_accordin/ttas_accordin/reciprocating_accordin lock cannot start." >&2
    return 1
  fi

  return 0
}

should_auto_sudo() {
  local lock_name="$1"
  local lock_script="${2:-}"

  case "$lock_name" in
    flexguard*|hybridlock*|mcs_accordin*|mcs_tas_accordin*|ttas_accordin*|reciprocating_accordin*)
      return 0
      ;;
  esac

  if [[ -n "$lock_script" ]]; then
    local script_base
    script_base="$(basename "$lock_script")"
    case "$script_base" in
      interpose_flexguard*.sh|interpose_hybridlock*.sh)
        return 0
        ;;
    esac
  fi

  return 1
}

append_accordin_env_args() {
  local -n env_args_ref="$1"

  if [[ -n "${ACCORDIN_CPU_MASK_K+x}" ]]; then
    env_args_ref+=("ACCORDIN_CPU_MASK_K=${ACCORDIN_CPU_MASK_K}")
  fi
  if [[ -n "${K+x}" ]]; then
    env_args_ref+=("K=${K}")
  fi
}

build_perf_symbols_package_if_needed() {
  local package="$1"
  local env_var_name="$2"

  if [[ "$profiling_enabled" != "1" || "$dry_run" == "1" ]]; then
    return 0
  fi
  if [[ "${MUTEXBENCH_PROFILE_SKIP_PERF_SYMBOLS_BUILD:-0}" == "1" ]]; then
    return 0
  fi
  if [[ -n "${!env_var_name:-}" ]]; then
    echo "[profile] ${env_var_name} is set; using caller-provided library without rebuilding perf-symbols." >&2
    return 0
  fi
  if ! command -v cargo >/dev/null 2>&1; then
    echo "cargo is required to build ${package} with --features perf-symbols for --profile." >&2
    echo "Install cargo, set ${env_var_name} to a prebuilt profiling library, or set MUTEXBENCH_PROFILE_SKIP_PERF_SYMBOLS_BUILD=1." >&2
    return 1
  fi

  echo "[profile] Building ${package} with release profile and perf-symbols..." >&2
  cargo build --manifest-path "$PROJECT_ROOT/Cargo.toml" -p "$package" --release --features perf-symbols >/dev/null
}

choose_perf_analysis_sudo() {
  local kptr_restrict="0"

  if [[ "$EUID" -eq 0 ]]; then
    printf "%s\n" "no"
    return 0
  fi

  if [[ -r /proc/sys/kernel/kptr_restrict ]]; then
    kptr_restrict="$(cat /proc/sys/kernel/kptr_restrict 2>/dev/null || printf "0")"
  fi
  if [[ "$kptr_restrict" == "0" ]]; then
    printf "%s\n" "no"
    return 0
  fi

  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
    printf "%s\n" "yes"
    return 0
  fi

  echo "[profile] Warning: /proc/sys/kernel/kptr_restrict=${kptr_restrict}; kernel symbols may stay unresolved in perf reports." >&2
  echo "[profile] Run with passwordless sudo available, run the sweep under sudo, or inspect manually with: sudo perf report --stdio -f -i <perf.data>" >&2
  printf "%s\n" "no"
}

run_perf_analysis_to_file() {
  local use_sudo="$1"
  local output_path="$2"
  shift 2

  if [[ "$use_sudo" == "yes" && "$EUID" -ne 0 ]]; then
    sudo -n -- "$@" > "$output_path"
  else
    "$@" > "$output_path"
  fi
}

generate_perf_reports_for_lock() {
  local lock_name="$1"
  local raw_csv="$2"
  local lock_dir="$3"
  local perf_report_dir="$lock_dir/perf_reports"
  local rows_path=""
  local analysis_sudo="no"
  local threads=""
  local critical_iters=""
  local outside_iters=""
  local repeat=""
  local perf_data_path=""
  local base=""
  local report_path=""
  local script_path=""
  local report_stderr_path=""
  local script_stderr_path=""
  local index_path="$perf_report_dir/index.csv"

  if [[ "$profiling_enabled" != "1" ]]; then
    return 0
  fi
  if ! command -v perf >/dev/null 2>&1; then
    echo "perf not found in PATH; cannot generate perf reports for lock=${lock_name}" >&2
    return 1
  fi
  if [[ ! -s "$raw_csv" ]]; then
    echo "raw.csv is missing or empty; cannot generate perf reports for lock=${lock_name}: $raw_csv" >&2
    return 1
  fi

  mkdir -p "$perf_report_dir"
  rows_path="$(mktemp)"
  awk -F',' '
    NR == 1 {
      for (i = 1; i <= NF; ++i) {
        if ($i == "perf_data_path") {
          perf_col = i
        }
      }
      if (perf_col == 0) {
        exit 2
      }
      next
    }
    perf_col > 0 && $perf_col != "" {
      printf "%s\t%s\t%s\t%s\t%s\n", $1, $2, $3, $4, $perf_col
    }
  ' "$raw_csv" > "$rows_path" || {
    rm -f -- "$rows_path"
    echo "raw.csv has no perf_data_path column; cannot generate perf reports for lock=${lock_name}: $raw_csv" >&2
    return 1
  }

  analysis_sudo="$(choose_perf_analysis_sudo)"
  printf "%s\n" "threads,critical_iters,outside_iters,repeat,perf_data_path,perf_report_path,perf_script_path,perf_report_status,perf_script_status" > "$index_path"

  while IFS=$'\t' read -r threads critical_iters outside_iters repeat perf_data_path; do
    [[ -z "$perf_data_path" ]] && continue
    if [[ ! -r "$perf_data_path" ]]; then
      rm -f -- "$rows_path"
      echo "perf.data is not readable for lock=${lock_name}: $perf_data_path" >&2
      return 1
    fi

    base="$(basename "$perf_data_path")"
    base="${base%.perf.data}"
    report_path="$perf_report_dir/${base}.report.txt"
    script_path="$perf_report_dir/${base}.script.txt"
    report_stderr_path="$perf_report_dir/${base}.report.stderr"
    script_stderr_path="$perf_report_dir/${base}.script.stderr"

    if ! run_perf_analysis_to_file "$analysis_sudo" "$report_path" \
      perf report --stdio -f -i "$perf_data_path" 2>"$report_stderr_path"; then
      rm -f -- "$rows_path"
      echo "Failed to generate perf report for lock=${lock_name}: $perf_data_path" >&2
      if [[ -s "$report_stderr_path" ]]; then
        cat "$report_stderr_path" >&2
      fi
      return 1
    fi

    if ! run_perf_analysis_to_file "$analysis_sudo" "$script_path" \
      perf script --demangle -f -F ip,sym,dso -i "$perf_data_path" 2>"$script_stderr_path"; then
      rm -f -- "$rows_path"
      echo "Failed to generate perf script for lock=${lock_name}: $perf_data_path" >&2
      if [[ -s "$script_stderr_path" ]]; then
        cat "$script_stderr_path" >&2
      fi
      return 1
    fi

    [[ -s "$report_stderr_path" ]] || rm -f -- "$report_stderr_path"
    [[ -s "$script_stderr_path" ]] || rm -f -- "$script_stderr_path"
    printf "%s,%s,%s,%s,%s,%s,%s,ok,ok\n" \
      "$threads" "$critical_iters" "$outside_iters" "$repeat" \
      "$perf_data_path" "$report_path" "$script_path" >> "$index_path"
    restore_output_owner_if_sudo_user "$report_path" "$script_path" "$report_stderr_path" "$script_stderr_path"
  done < "$rows_path"

  rm -f -- "$rows_path"
  restore_output_owner_if_sudo_user "$index_path"
  echo "Perf reports: $perf_report_dir" >&2
}

run_bpftool_capture() {
  local use_sudo="$1"
  shift

  if [[ "$use_sudo" == "yes" && "$EUID" -ne 0 ]]; then
    sudo -n -- bpftool "$@"
  else
    bpftool "$@"
  fi
}

run_bpftool_to_file() {
  local use_sudo="$1"
  local output_path="$2"
  shift 2

  if [[ "$use_sudo" == "yes" && "$EUID" -ne 0 ]]; then
    sudo -n -- bpftool "$@" > "$output_path"
  else
    bpftool "$@" > "$output_path"
  fi
}

latest_bpf_prog_id() {
  local use_sudo="$1"
  local program_name="$2"
  local out=""

  out="$(run_bpftool_capture "$use_sudo" prog list name "$program_name" 2>/dev/null || true)"
  if [[ -z "$out" ]]; then
    return 0
  fi

  awk -F':' '/^[0-9]+:/ { if (($1 + 0) > max) max = $1 + 0 } END { if (max > 0) print max }' <<< "$out"
}

capture_latest_bpf_prog_ids() {
  local use_sudo="$1"
  local names_var="$2"
  local ids_var="$3"
  local -n names_ref="$names_var"
  local -n ids_ref="$ids_var"
  local name=""
  local id=""

  ids_ref=()
  for name in "${names_ref[@]}"; do
    id="$(latest_bpf_prog_id "$use_sudo" "$name")"
    ids_ref+=("${id:-0}")
  done
}

wait_for_new_bpf_profile_programs() {
  local use_sudo="$1"
  local names_var="$2"
  local baseline_ids_var="$3"
  local -n names_ref="$names_var"
  local -n baseline_ids_ref="$baseline_ids_var"
  local attempts=$(( bpf_profile_wait_timeout_s * 10 ))
  local attempt=0
  local i=0
  local id=""
  local baseline_id=""

  for ((attempt = 0; attempt < attempts; ++attempt)); do
    for ((i = 0; i < ${#names_ref[@]}; ++i)); do
      id="$(latest_bpf_prog_id "$use_sudo" "${names_ref[$i]}")"
      baseline_id="${baseline_ids_ref[$i]:-0}"
      if [[ -n "$id" && "$id" -gt "$baseline_id" ]]; then
        return 0
      fi
    done
    sleep 0.1
  done

  return 1
}

parse_bpf_profile_metric() {
  local path="$1"
  local metric="$2"

  awk -v metric="$metric" '$2 == metric { print $1; found = 1; exit } END { if (!found) print "0" }' "$path"
}

format_metric_per_run() {
  local numerator="$1"
  local run_cnt="$2"

  awk -v numerator="$numerator" -v run_cnt="$run_cnt" 'BEGIN {
    if (run_cnt > 0) {
      printf "%.2f", numerator / run_cnt
    } else {
      printf "0.00"
    }
  }'
}

collect_bpf_profile_sample() {
  local lock_name="$1"
  local lock_dir="$2"
  local use_sudo="$3"
  local sample="$4"
  local names_var="$5"
  local baseline_ids_var="$6"
  local -n names_ref="$names_var"
  local -n baseline_ids_ref="$baseline_ids_var"
  local bpf_profile_dir="$lock_dir/bpf_profiles"
  local index_path="$bpf_profile_dir/index.csv"
  local i=0
  local name=""
  local id=""
  local baseline_id=""
  local profile_path=""
  local stderr_path=""
  local run_cnt=""
  local cycles=""
  local instructions=""
  local cycles_per_run=""
  local instructions_per_run=""
  local profile_status=0
  local wait_rc=0
  local -a pids=()
  local -a pid_names=()
  local -a pid_ids=()
  local -a pid_profile_paths=()
  local -a pid_stderr_paths=()

  mkdir -p "$bpf_profile_dir"
  if [[ ! -e "$index_path" ]]; then
    printf "%s\n" "sample,program_name,prog_id,profile_path,status,run_cnt,cycles,instructions,cycles_per_run,instructions_per_run" > "$index_path"
  fi

  for ((i = 0; i < ${#names_ref[@]}; ++i)); do
    name="${names_ref[$i]}"
    baseline_id="${baseline_ids_ref[$i]:-0}"
    id="$(latest_bpf_prog_id "$use_sudo" "$name")"
    profile_path="$bpf_profile_dir/sample${sample}_${name}.profile.txt"
    stderr_path="$bpf_profile_dir/sample${sample}_${name}.profile.stderr"

    if [[ -z "$id" || "$id" -le "$baseline_id" ]]; then
      : > "$profile_path"
      printf "%s,%s,%s,%s,missing,0,0,0,0.00,0.00\n" \
        "$sample" "$name" "${id:-}" "$profile_path" >> "$index_path"
      continue
    fi

    (
      run_bpftool_to_file "$use_sudo" "$profile_path" \
        prog profile id "$id" duration "$bpf_profile_duration_s" cycles instructions
    ) 2>"$stderr_path" &
    pids+=("$!")
    pid_names+=("$name")
    pid_ids+=("$id")
    pid_profile_paths+=("$profile_path")
    pid_stderr_paths+=("$stderr_path")
  done

  for ((i = 0; i < ${#pids[@]}; ++i)); do
    set +e
    wait "${pids[$i]}"
    wait_rc=$?
    set -e

    name="${pid_names[$i]}"
    id="${pid_ids[$i]}"
    profile_path="${pid_profile_paths[$i]}"
    stderr_path="${pid_stderr_paths[$i]}"

    if [[ "$wait_rc" -ne 0 ]]; then
      profile_status=1
      printf "%s,%s,%s,%s,failed,0,0,0,0.00,0.00\n" \
        "$sample" "$name" "$id" "$profile_path" >> "$index_path"
      if [[ -s "$stderr_path" ]]; then
        echo "Failed to profile BPF program ${name} (id=${id}) for lock=${lock_name}:" >&2
        cat "$stderr_path" >&2
      fi
      continue
    fi

    run_cnt="$(parse_bpf_profile_metric "$profile_path" "run_cnt")"
    cycles="$(parse_bpf_profile_metric "$profile_path" "cycles")"
    instructions="$(parse_bpf_profile_metric "$profile_path" "instructions")"
    cycles_per_run="$(format_metric_per_run "$cycles" "$run_cnt")"
    instructions_per_run="$(format_metric_per_run "$instructions" "$run_cnt")"
    printf "%s,%s,%s,%s,ok,%s,%s,%s,%s,%s\n" \
      "$sample" "$name" "$id" "$profile_path" "$run_cnt" "$cycles" \
      "$instructions" "$cycles_per_run" "$instructions_per_run" >> "$index_path"

    [[ -s "$stderr_path" ]] || rm -f -- "$stderr_path"
  done

  restore_output_owner_if_sudo_user "$index_path" "$bpf_profile_dir" "${pid_profile_paths[@]}" "${pid_stderr_paths[@]}"
  echo "BPF profiles: $bpf_profile_dir" >&2
  return "$profile_status"
}

extract_schedule_pct_of_total() {
  local lock_dir="$1"
  local perf_index="$lock_dir/perf_reports/index.csv"
  local reports_path=""
  local schedule_pct=""

  if [[ ! -s "$perf_index" ]]; then
    return 0
  fi

  reports_path="$(mktemp)"
  awk -F',' '
    NR == 1 {
      for (i = 1; i <= NF; ++i) {
        if ($i == "perf_report_path") {
          report_col = i
        }
        if ($i == "perf_report_status") {
          status_col = i
        }
      }
      next
    }
    report_col > 0 && (status_col == 0 || $status_col == "ok") && $report_col != "" {
      print $report_col
    }
  ' "$perf_index" > "$reports_path"

  schedule_pct="$(
    while IFS= read -r report_path; do
      [[ -s "$report_path" ]] || continue
      awk '
        /(^|[[:space:]])__schedule([[:space:]]|$)/ {
          pct = $1
          gsub("%", "", pct)
          if (pct ~ /^[0-9.]+$/) {
            print pct
            exit
          }
        }
      ' "$report_path"
    done < "$reports_path" |
    awk '{ sum += $1; count += 1 } END { if (count > 0) printf "%.6f", sum / count }'
  )"

  rm -f -- "$reports_path"
  [[ -n "$schedule_pct" ]] && printf "%s\n" "$schedule_pct"
}

generate_bpf_profile_summary_for_lock() {
  local lock_name="$1"
  local lock_dir="$2"
  local bpf_profile_dir="$lock_dir/bpf_profiles"
  local index_path="$bpf_profile_dir/index.csv"
  local summary_path="$bpf_profile_dir/summary.csv"
  local schedule_pct=""
  local body_path=""

  if [[ "$bpf_profile_enabled" != "1" || ! -s "$index_path" ]]; then
    return 0
  fi

  schedule_pct="$(extract_schedule_pct_of_total "$lock_dir" || true)"
  body_path="$(mktemp)"
  awk -F',' -v schedule_pct="$schedule_pct" '
    NR == 1 {
      for (i = 1; i <= NF; ++i) {
        if ($i == "program_name") {
          program_col = i
        } else if ($i == "status") {
          status_col = i
        } else if ($i == "run_cnt") {
          run_col = i
        } else if ($i == "cycles") {
          cycles_col = i
        } else if ($i == "instructions") {
          instructions_col = i
        }
      }
      next
    }
    status_col > 0 && $status_col == "ok" {
      program = $program_col
      if (!(program in seen)) {
        seen[program] = 1
        order[++order_count] = program
      }
      samples[program] += 1
      run_cnt[program] += $run_col
      cycles[program] += $cycles_col
      instructions[program] += $instructions_col
      total_cycles += $cycles_col
    }
    END {
      for (i = 1; i <= order_count; ++i) {
        program = order[i]
        function_pct = total_cycles > 0 ? cycles[program] * 100.0 / total_cycles : 0
        cycles_per_run = run_cnt[program] > 0 ? cycles[program] / run_cnt[program] : 0
        instructions_per_run = run_cnt[program] > 0 ? instructions[program] / run_cnt[program] : 0
        if (schedule_pct != "") {
          estimated_pct = schedule_pct * function_pct / 100.0
          printf "%s,%d,%.0f,%.0f,%.0f,%.6f,%.6f,%.6f,%.2f,%.2f\n",
            program, samples[program], run_cnt[program], cycles[program],
            instructions[program], function_pct, schedule_pct, estimated_pct,
            cycles_per_run, instructions_per_run
        } else {
          printf "%s,%d,%.0f,%.0f,%.0f,%.6f,,,%.2f,%.2f\n",
            program, samples[program], run_cnt[program], cycles[program],
            instructions[program], function_pct, cycles_per_run,
            instructions_per_run
        }
      }
    }
  ' "$index_path" | sort -t',' -k4,4nr > "$body_path"

  printf "%s\n" "program_name,profile_samples,run_cnt,cycles,instructions,function_pct_of_profiled_schedule,schedule_pct_of_total,estimated_function_pct_of_total,cycles_per_run,instructions_per_run" > "$summary_path"
  cat "$body_path" >> "$summary_path"
  rm -f -- "$body_path"
  restore_output_owner_if_sudo_user "$summary_path"
  echo "BPF profile summary: $summary_path" >&2
}

run_command_with_optional_bpf_profile() {
  local lock_name="$1"
  local lock_dir="$2"
  local use_sudo="$3"
  local profile_this_lock="$4"
  shift 4
  local -a command=("$@")
  local -a bpf_profile_names=()
  local -a baseline_ids=()
  local command_pid=""
  local command_rc=0
  local profile_rc=0

  if [[ "$profile_this_lock" != "1" ]]; then
    "${command[@]}"
    return $?
  fi

  if ! command -v bpftool >/dev/null 2>&1; then
    echo "bpftool not found in PATH; cannot run --bpf-profile for lock=${lock_name}" >&2
    return 1
  fi

  split_csv_values "$bpf_profile_programs" bpf_profile_names
  if [[ ${#bpf_profile_names[@]} -eq 0 ]]; then
    echo "--bpf-profile-programs must contain at least one program name" >&2
    return 1
  fi

  capture_latest_bpf_prog_ids "$use_sudo" bpf_profile_names baseline_ids
  "${command[@]}" &
  command_pid="$!"

  if wait_for_new_bpf_profile_programs "$use_sudo" bpf_profile_names baseline_ids; then
    collect_bpf_profile_sample "$lock_name" "$lock_dir" "$use_sudo" "1" bpf_profile_names baseline_ids || profile_rc=$?
  else
    echo "Timed out waiting for new Accordin BPF programs for lock=${lock_name}; cannot collect --bpf-profile." >&2
    profile_rc=1
  fi

  set +e
  wait "$command_pid"
  command_rc=$?
  set -e

  if [[ "$command_rc" -ne 0 ]]; then
    return "$command_rc"
  fi
  return "$profile_rc"
}

scx_lavd_started="0"
scx_lavd_pid=""
declare -a scx_lavd_cmd=()

stop_scx_lavd_if_started() {
  if [[ "$scx_lavd_started" != "1" ]]; then
    return 0
  fi

  echo "[scx_lavd] Stopping background scheduler..." >&2
  if [[ -n "$scx_lavd_pid" ]]; then
    run_with_optional_sudo "yes" kill -TERM "$scx_lavd_pid" >/dev/null 2>&1 || true
  fi

  local i=0
  for ((i = 0; i < 50; ++i)); do
    if [[ -z "$scx_lavd_pid" ]] || ! run_with_optional_sudo "yes" kill -0 "$scx_lavd_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.1
  done

  if [[ -n "$scx_lavd_pid" ]] && run_with_optional_sudo "yes" kill -0 "$scx_lavd_pid" >/dev/null 2>&1; then
    run_with_optional_sudo "yes" kill -KILL "$scx_lavd_pid" >/dev/null 2>&1 || true
  fi

  if [[ -n "$scx_lavd_pid" ]]; then
    wait "$scx_lavd_pid" 2>/dev/null || true
  fi

  local state=""
  local ops=""
  state="$(sched_ext_state || true)"
  if [[ "$state" == "enabled" ]]; then
    ops="$(sched_ext_ops_name || true)"
    local -A seen_owner_pids=()
    local -a owner_pids=()
    local id=""
    local pid=""

    while IFS= read -r id; do
      [[ -z "$id" ]] && continue
      while IFS= read -r pid; do
        [[ -z "$pid" ]] && continue
        [[ -n "${seen_owner_pids[$pid]:-}" ]] && continue
        if run_with_optional_sudo "yes" kill -0 "$pid" >/dev/null 2>&1; then
          seen_owner_pids["$pid"]=1
          owner_pids+=("$pid")
        fi
      done < <(list_sched_ext_owner_pids_by_map_id "yes" "$id")
    done < <(list_sched_ext_struct_ops_ids "yes")

    if [[ ${#owner_pids[@]} -gt 0 ]]; then
      echo "[scx_lavd] sched_ext still enabled (ops=${ops:-unknown}); stopping owner PIDs: ${owner_pids[*]}" >&2
      for pid in "${owner_pids[@]}"; do
        run_with_optional_sudo "yes" kill -TERM "$pid" >/dev/null 2>&1 || true
      done

      local i=0
      for ((i = 0; i < 50; ++i)); do
        state="$(sched_ext_state || true)"
        if [[ "$state" != "enabled" ]]; then
          break
        fi
        sleep 0.1
      done

      if [[ "$state" == "enabled" ]]; then
        for pid in "${owner_pids[@]}"; do
          run_with_optional_sudo "yes" kill -KILL "$pid" >/dev/null 2>&1 || true
        done
      fi
    fi
  fi

  state="$(sched_ext_state || true)"
  if [[ "$state" == "enabled" ]]; then
    ops="$(sched_ext_ops_name || true)"
    echo "[scx_lavd] Warning: sched_ext is still enabled after cleanup (ops=${ops:-unknown})." >&2
  fi

  scx_lavd_started="0"
  scx_lavd_pid=""
}

start_scx_lavd() {
  local scx_lavd_bin="$1"
  local state=""
  local ops=""

  state="$(sched_ext_state || true)"
  if [[ "$state" == "enabled" ]]; then
    ops="$(sched_ext_ops_name || true)"
    local owners="unknown"
    owners="$(format_sched_ext_owner_diag "yes" || true)"
    echo "Cannot start scx_lavd: sched_ext is already enabled (ops=${ops:-unknown}, owners=${owners})." >&2
    echo "Stop existing sched_ext owner first." >&2
    return 1
  fi

  scx_lavd_cmd=("$scx_lavd_bin" --per-cpu-dsq --performance)
  echo "[scx_lavd] Starting background scheduler..." >&2
  if [[ "$EUID" -ne 0 ]]; then
    sudo -- "${scx_lavd_cmd[@]}" &
  else
    "${scx_lavd_cmd[@]}" &
  fi
  scx_lavd_pid="$!"
  scx_lavd_started="1"

  local i=0
  for ((i = 0; i < 50; ++i)); do
    if ! kill -0 "$scx_lavd_pid" >/dev/null 2>&1; then
      wait "$scx_lavd_pid" 2>/dev/null || true
      scx_lavd_started="0"
      scx_lavd_pid=""
      echo "scx_lavd exited before sched_ext became enabled." >&2
      return 1
    fi

    state="$(sched_ext_state || true)"
    if [[ "$state" == "enabled" ]]; then
      break
    fi
    sleep 0.1
  done

  state="$(sched_ext_state || true)"
  if [[ "$state" != "enabled" ]]; then
    echo "scx_lavd did not enable sched_ext in time." >&2
    stop_scx_lavd_if_started
    return 1
  fi

  ops="$(sched_ext_ops_name || true)"
  echo "[scx_lavd] Running (pid=${scx_lavd_pid}, ops=${ops:-unknown})." >&2
  return 0
}

locks_csv=""
sweep_script="$SCRIPT_DIR/sweep_mutex_throughput.sh"
output_root="$MUTEXBENCH_DIR/results"
profiling_enabled="0"
bpf_profile_enabled="0"
bpf_profile_duration_s="2"
bpf_profile_wait_timeout_s="10"
bpf_profile_programs="accordin_select_cpu,accordin_enqueue,accordin_dispatch,accordin_set_active_cpus,accordin_nudge_cpu,accordin_running,accordin_tick,accordin_stopping,accordin_exit_task,accordin_init,accordin_exit"
sample_bpf_enabled="0"
sample_bpf_layout="auto"
sample_bpf_interval_us="500"
sudo_mode="all"
litl_dir="$LITL_DIR"
with_scx_lavd="0"
scx_lavd_bin="/mnt/home/jz/scx/target/release/scx_lavd"
mcs_tas_accordin_sched_ext_conflict="stop"
dry_run="0"
queue_lock_file="${MUTEXBENCH_MULTI_LOCK_LOCK_FILE:-/tmp/mutexbench-sweep-multi-lock.lock}"
declare -a sweep_args=()

ensure_accordin_sched_ext_ready() {
  ensure_mcs_tas_accordin_sched_ext_ready "$@"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --locks)
      locks_csv="${2:-}"
      shift 2
      ;;
    --sweep-script)
      sweep_script="${2:-}"
      shift 2
      ;;
    --output-root)
      output_root="${2:-}"
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
    --bpf-profile)
      if [[ $# -gt 1 && -n "${2:-}" && "${2:0:1}" != "-" ]]; then
        echo "--bpf-profile does not take a value; use bare --bpf-profile" >&2
        exit 1
      fi
      bpf_profile_enabled="1"
      shift
      ;;
    --bpf-profile-duration-s)
      bpf_profile_duration_s="${2:-}"
      shift 2
      ;;
    --bpf-profile-programs)
      bpf_profile_programs="${2:-}"
      shift 2
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
    --sudo-mode)
      sudo_mode="${2:-}"
      shift 2
      ;;
    --litl-dir)
      litl_dir="${2:-}"
      shift 2
      ;;
    --with-scx-lavd)
      with_scx_lavd="1"
      shift
      ;;
    --scx-lavd-bin)
      scx_lavd_bin="${2:-}"
      shift 2
      ;;
    --lb-accordin-sched-ext-conflict)
      mcs_tas_accordin_sched_ext_conflict="${2:-}"
      shift 2
      ;;
    --dry-run)
      dry_run="1"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      sweep_args+=("$@")
      break
      ;;
    *)
      sweep_args+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$locks_csv" ]]; then
  echo "--locks is required" >&2
  usage >&2
  exit 1
fi

if contains_flag "--output-raw" "${sweep_args[@]}"; then
  echo "Do not pass --output-raw. It is generated per lock." >&2
  exit 1
fi
if contains_flag "--output-summary" "${sweep_args[@]}"; then
  echo "Do not pass --output-summary. It is generated per lock." >&2
  exit 1
fi
if contains_flag "--litl-lock" "${sweep_args[@]}"; then
  echo "Do not pass --litl-lock through forwarded sweep args. Use a litl:<name> entry in --locks instead." >&2
  exit 1
fi
if [[ "$sudo_mode" != "all" && "$sudo_mode" != "auto" && "$sudo_mode" != "none" ]]; then
  echo "--sudo-mode must be one of: all, auto, none" >&2
  exit 1
fi
if [[ "$mcs_tas_accordin_sched_ext_conflict" != "stop" && "$mcs_tas_accordin_sched_ext_conflict" != "error" && "$mcs_tas_accordin_sched_ext_conflict" != "ignore" ]]; then
  echo "--lb-accordin-sched-ext-conflict must be one of: stop, error, ignore" >&2
  exit 1
fi
if ! is_positive_uint "$bpf_profile_duration_s"; then
  echo "--bpf-profile-duration-s must be an integer > 0" >&2
  exit 1
fi
if ! is_positive_uint "$bpf_profile_wait_timeout_s"; then
  echo "internal error: bpf_profile_wait_timeout_s must be an integer > 0" >&2
  exit 1
fi

queue_lock_file="$(expand_home "$queue_lock_file")"
if [[ "$queue_lock_file" != /* ]]; then
  queue_lock_file="$MUTEXBENCH_DIR/$queue_lock_file"
fi
acquire_queue_lock "$queue_lock_file"

if [[ "$profiling_enabled" == "1" ]]; then
  sweep_args+=(--profile)
fi

sweep_script="$(resolve_executable_path "$sweep_script" "$MUTEXBENCH_DIR")"
if [[ ! -x "$sweep_script" ]]; then
  echo "Sweep script is not executable: $sweep_script" >&2
  exit 1
fi
if [[ "$with_scx_lavd" == "1" ]]; then
  scx_lavd_bin="$(resolve_executable_path "$scx_lavd_bin" "$MUTEXBENCH_DIR")"
  if [[ ! -x "$scx_lavd_bin" ]]; then
    echo "scx_lavd binary is not executable: $scx_lavd_bin" >&2
    exit 1
  fi
  if [[ "$dry_run" != "1" ]]; then
    trap stop_scx_lavd_if_started EXIT
  fi
fi

output_root="$(expand_home "$output_root")"
if [[ "$output_root" != /* ]]; then
  output_root="$MUTEXBENCH_DIR/$output_root"
fi
mkdir -p "$output_root"

declare -a lock_items=()
IFS=',' read -r -a lock_items <<< "$locks_csv"
if [[ ${#lock_items[@]} -eq 0 ]]; then
  echo "No lock scripts in --locks" >&2
  exit 1
fi

if [[ "$dry_run" == "1" && "$with_scx_lavd" == "1" ]]; then
  scx_lavd_cmd=("$scx_lavd_bin" --per-cpu-dsq --performance)
  if [[ "$EUID" -ne 0 ]]; then
    scx_lavd_cmd=(sudo -- "${scx_lavd_cmd[@]}")
  fi
  echo "=== scx_lavd=enabled mode=background ===" >&2
  printf 'Command:' >&2
  printf ' %q' "${scx_lavd_cmd[@]}" >&2
  printf '\n' >&2
fi

declare -A seen_names=()

for item in "${lock_items[@]}"; do
  item="$(trim_spaces "$item")"
  if [[ -z "$item" ]]; then
    echo "Empty item in --locks" >&2
    exit 1
  fi

  lock_name=""
  lock_script=""
  lock_kind="hook"
  bench_lock_kind=""
  litl_lock_name=""
  litl_launcher=""
  mcs_accordin_lib=""
  mcs_tse_lib=""
  mcs_tas_accordin_direct_lib=""
  ttas_accordin_lib=""
  reciprocating_accordin_lib=""
  mcs_accordin_disable_bpf="0"
  mcs_tas_accordin_direct_disable_bpf="0"
  ttas_accordin_disable_bpf="0"
  reciprocating_accordin_disable_bpf="0"
  lock_bpf_profile_enabled="0"
  if [[ "$item" == *=* ]]; then
    lock_name="${item%%=*}"
    lock_script="${item#*=}"
    lock_name="$(trim_spaces "$lock_name")"
    lock_script="$(trim_spaces "$lock_script")"
  else
    case "$item" in
      native-mutex)
        lock_kind="native"
        lock_name="mutex"
        bench_lock_kind="mutex"
        lock_script=""
        ;;
      litl:*)
        lock_kind="litl"
        lock_script=""
        litl_lock_name="${item#litl:}"
        if [[ -z "$litl_lock_name" ]]; then
          echo "Empty LiTL library name in item: $item" >&2
          exit 1
        fi
        lock_name="$litl_lock_name"
        ;;
      native:*)
        lock_kind="native"
        lock_script=""
        bench_lock_kind="${item#native:}"
        if is_builtin_lock_kind "$bench_lock_kind"; then
          lock_name="$bench_lock_kind"
        else
          echo "Invalid native lock kind in item: $item" >&2
          exit 1
        fi
        ;;
      mcs_accordin)
        lock_kind="mcs_accordin_direct"
        lock_name="mcs_accordin"
        lock_script=""
        ;;
      mcs_accordin_no_bpf)
        lock_kind="mcs_accordin_direct"
        lock_name="mcs_accordin_no_bpf"
        lock_script=""
        mcs_accordin_disable_bpf="1"
        ;;
      mcs_tse)
        lock_kind="mcs_tse"
        lock_name="mcs_tse"
        lock_script=""
        ;;
      mcs_tas_accordin)
        lock_kind="mcs_tas_accordin_direct"
        lock_name="mcs_tas_accordin"
        lock_script=""
        ;;
      mcs_tas_accordin_no_bpf)
        lock_kind="mcs_tas_accordin_direct"
        lock_name="mcs_tas_accordin_no_bpf"
        lock_script=""
        mcs_tas_accordin_direct_disable_bpf="1"
        ;;
      ttas_accordin)
        lock_kind="ttas_accordin"
        lock_name="ttas_accordin"
        lock_script=""
        ;;
      ttas_accordin_no_bpf)
        lock_kind="ttas_accordin"
        lock_name="ttas_accordin_no_bpf"
        lock_script=""
        ttas_accordin_disable_bpf="1"
        ;;
      reciprocating_accordin)
        lock_kind="reciprocating_accordin"
        lock_name="reciprocating_accordin"
        lock_script=""
        ;;
      reciprocating_accordin_no_bpf)
        lock_kind="reciprocating_accordin"
        lock_name="reciprocating_accordin_no_bpf"
        lock_script=""
        reciprocating_accordin_disable_bpf="1"
        ;;
      *)
        if is_builtin_lock_kind "$item"; then
          lock_kind="native"
          lock_name="$item"
          bench_lock_kind="$item"
          lock_script=""
        else
          lock_script="$item"
          lock_base="$(basename "$lock_script")"
          lock_name="${lock_base%.sh}"
          lock_name="${lock_name#interpose_}"
        fi
      ;;
    esac
  fi

  if [[ "$lock_kind" == "hook" && "$lock_script" != */* && "$lock_script" != *.sh && -n "${FLEXGUARD_DIR:-}" ]]; then
    resolved_short_lock_script=""
    if resolved_short_lock_script="$(resolve_flexguard_short_lock_script "$lock_script")"; then
      lock_script="$resolved_short_lock_script"
    fi
  fi

  if [[ -z "$lock_name" || ( "$lock_kind" == "hook" && -z "$lock_script" ) ]]; then
    echo "Invalid lock item: $item" >&2
    exit 1
  fi
  if [[ "$lock_kind" == "native" && -z "${bench_lock_kind:-}" ]]; then
    bench_lock_kind="mutex"
  fi
  if [[ "$lock_name" =~ [^A-Za-z0-9._-] ]]; then
    echo "Invalid lock name '$lock_name' in item: $item" >&2
    exit 1
  fi
  if [[ -n "${seen_names[$lock_name]:-}" ]]; then
    echo "Duplicate lock name: $lock_name" >&2
    exit 1
  fi
  seen_names["$lock_name"]=1

  if [[ "$lock_kind" == "hook" ]]; then
    lock_script="$(resolve_executable_path "$lock_script" "$MUTEXBENCH_DIR")"
    if [[ ! -x "$lock_script" ]]; then
      echo "Lock script is not executable: $lock_script" >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "litl" ]]; then
    litl_launcher="$(require_litl_launcher "$litl_lock_name")"
  elif [[ "$lock_kind" == "mcs_accordin_direct" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    build_perf_symbols_package_if_needed "mcs_accordin_direct" "MCS_ACCORDIN_DIRECT_LIB"
    mcs_accordin_lib="$(resolve_mcs_accordin_lib_path)"
    if [[ ! -f "$mcs_accordin_lib" ]]; then
      echo "mcs_accordin_direct library not found: $mcs_accordin_lib" >&2
      echo "Build first (cargo build -p mcs_accordin_direct --release) or set MCS_ACCORDIN_DIRECT_LIB to libmcs_accordin_direct.so path." >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "mcs_tse" ]]; then
    build_perf_symbols_package_if_needed "mcs_tse" "MCS_TSE_LIB"
    mcs_tse_lib="$(resolve_mcs_tse_lib_path)"
    if [[ ! -f "$mcs_tse_lib" && "$dry_run" != "1" ]]; then
      echo "mcs_tse library not found: $mcs_tse_lib" >&2
      echo "Build first (cargo build -p mcs_tse --release) or set MCS_TSE_LIB to libmcs_tse.so path." >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "mcs_tas_accordin_direct" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    build_perf_symbols_package_if_needed "mcs_tas_accordin_direct" "MCS_TAS_ACCORDIN_DIRECT_LIB"
    mcs_tas_accordin_direct_lib="$(resolve_mcs_tas_accordin_direct_lib_path)"
    if [[ ! -f "$mcs_tas_accordin_direct_lib" && "$dry_run" != "1" ]]; then
      echo "mcs_tas_accordin_direct library not found: $mcs_tas_accordin_direct_lib" >&2
      echo "Build first (cargo build -p mcs_tas_accordin_direct --release) or set MCS_TAS_ACCORDIN_DIRECT_LIB to libmcs_tas_accordin_direct.so path." >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "ttas_accordin" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    build_perf_symbols_package_if_needed "ttas_accordin" "TTAS_ACCORDIN_LIB"
    ttas_accordin_lib="$(resolve_ttas_accordin_lib_path)"
    if [[ ! -f "$ttas_accordin_lib" ]]; then
      echo "ttas_accordin library not found: $ttas_accordin_lib" >&2
      echo "Build first (cargo build -p ttas_accordin --release) or set TTAS_ACCORDIN_LIB to libttas_accordin.so path." >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "reciprocating_accordin" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    build_perf_symbols_package_if_needed "reciprocating_accordin" "RECIPROCATING_ACCORDIN_LIB"
    reciprocating_accordin_lib="$(resolve_reciprocating_accordin_lib_path)"
    if [[ ! -f "$reciprocating_accordin_lib" ]]; then
      echo "reciprocating_accordin library not found: $reciprocating_accordin_lib" >&2
      echo "Build first (cargo build -p reciprocating_accordin --release) or set RECIPROCATING_ACCORDIN_LIB to libreciprocating_accordin.so path." >&2
      exit 1
    fi
  fi

  lock_dir="${output_root}/${lock_name}"
  raw_out="${lock_dir}/raw.csv"
  summary_out="${lock_dir}/summary.csv"
  mkdir -p "$lock_dir"

  sample_bpf_args=()
  if [[ "$sample_bpf_enabled" == "1" ]]; then
    case "$lock_kind" in
      mcs_accordin_direct)
        if [[ "$mcs_accordin_disable_bpf" != "1" ]]; then
          sample_bpf_args=(
            --sample-bpf
            --sample-bpf-layout "$sample_bpf_layout"
            --sample-bpf-interval-us "$sample_bpf_interval_us"
          )
        fi
        ;;
      mcs_tas_accordin_direct)
        if [[ "$mcs_tas_accordin_direct_disable_bpf" != "1" ]]; then
          sample_bpf_args=(
            --sample-bpf
            --sample-bpf-layout "$sample_bpf_layout"
            --sample-bpf-interval-us "$sample_bpf_interval_us"
          )
        fi
        ;;
      ttas_accordin)
        if [[ "$ttas_accordin_disable_bpf" != "1" ]]; then
          sample_bpf_args=(
            --sample-bpf
            --sample-bpf-layout "$sample_bpf_layout"
            --sample-bpf-interval-us "$sample_bpf_interval_us"
          )
        fi
        ;;
      reciprocating_accordin)
        if [[ "$reciprocating_accordin_disable_bpf" != "1" ]]; then
          sample_bpf_args=(
            --sample-bpf
            --sample-bpf-layout "$sample_bpf_layout"
            --sample-bpf-interval-us "$sample_bpf_interval_us"
          )
        fi
        ;;
    esac
  fi

  if [[ "$bpf_profile_enabled" == "1" ]]; then
    case "$lock_kind" in
      mcs_accordin_direct)
        if [[ "$mcs_accordin_disable_bpf" != "1" ]]; then
          lock_bpf_profile_enabled="1"
        fi
        ;;
      mcs_tas_accordin_direct)
        if [[ "$mcs_tas_accordin_direct_disable_bpf" != "1" ]]; then
          lock_bpf_profile_enabled="1"
        fi
        ;;
      ttas_accordin)
        if [[ "$ttas_accordin_disable_bpf" != "1" ]]; then
          lock_bpf_profile_enabled="1"
        fi
        ;;
      reciprocating_accordin)
        if [[ "$reciprocating_accordin_disable_bpf" != "1" ]]; then
          lock_bpf_profile_enabled="1"
        fi
        ;;
    esac
  fi

  accordin_env_args=()
  append_accordin_env_args accordin_env_args

  if [[ "$lock_kind" == "native" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --lock-kind "$bench_lock_kind"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  elif [[ "$lock_kind" == "litl" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --lock-kind "mutex"
      --litl-lock "$litl_launcher"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  elif [[ "$lock_kind" == "mcs_accordin_direct" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --lock-kind "mutex"
      --litl-lock "$(require_litl_launcher mcsaccordin_original)"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
    if [[ "$mcs_accordin_disable_bpf" == "1" ]]; then
      cmd=(env "${accordin_env_args[@]}" "MCS_ACCORDIN_DIRECT_LIB=${mcs_accordin_lib}" "MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS=${MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-}" "MCS_ACCORDIN_DIRECT_DISABLE_BPF=1" "${cmd[@]}")
    else
      cmd=(env "${accordin_env_args[@]}" "MCS_ACCORDIN_DIRECT_LIB=${mcs_accordin_lib}" "MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS=${MCS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-}" "${cmd[@]}")
    fi
  elif [[ "$lock_kind" == "mcs_tse" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --bench-ld-preload "$mcs_tse_lib"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  elif [[ "$lock_kind" == "mcs_tas_accordin_direct" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --lock-kind "mutex"
      --litl-lock "$(require_litl_launcher mcstasaccordin_original)"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
    if [[ "$mcs_tas_accordin_direct_disable_bpf" == "1" ]]; then
      cmd=(env "${accordin_env_args[@]}" "MCS_TAS_ACCORDIN_DIRECT_LIB=${mcs_tas_accordin_direct_lib}" "MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS=${MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-}" "MCS_TAS_ACCORDIN_DIRECT_DISABLE_BPF=1" "${cmd[@]}")
    else
      cmd=(env "${accordin_env_args[@]}" "MCS_TAS_ACCORDIN_DIRECT_LIB=${mcs_tas_accordin_direct_lib}" "MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS=${MCS_TAS_ACCORDIN_DIRECT_DEBUG_COUNTERS:-}" "${cmd[@]}")
    fi
  elif [[ "$lock_kind" == "ttas_accordin" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --bench-ld-preload "$ttas_accordin_lib"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
    if [[ "$ttas_accordin_disable_bpf" == "1" ]]; then
      cmd=(env "${accordin_env_args[@]}" "TTAS_ACCORDIN_DEBUG_COUNTERS=${TTAS_ACCORDIN_DEBUG_COUNTERS:-}" "TTAS_ACCORDIN_DISABLE_BPF=1" "${cmd[@]}")
    else
      cmd=(env "${accordin_env_args[@]}" "TTAS_ACCORDIN_DEBUG_COUNTERS=${TTAS_ACCORDIN_DEBUG_COUNTERS:-}" "${cmd[@]}")
    fi
  elif [[ "$lock_kind" == "reciprocating_accordin" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --bench-ld-preload "$reciprocating_accordin_lib"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
    if [[ "$reciprocating_accordin_disable_bpf" == "1" ]]; then
      cmd=(env "${accordin_env_args[@]}" "RECIPROCATING_ACCORDIN_DEBUG_COUNTERS=${RECIPROCATING_ACCORDIN_DEBUG_COUNTERS:-}" "RECIPROCATING_ACCORDIN_DISABLE_BPF=1" "${cmd[@]}")
    else
      cmd=(env "${accordin_env_args[@]}" "RECIPROCATING_ACCORDIN_DEBUG_COUNTERS=${RECIPROCATING_ACCORDIN_DEBUG_COUNTERS:-}" "${cmd[@]}")
    fi
  else
    cmd=(
      "$lock_script"
      "$sweep_script"
      "${sweep_args[@]}"
      "${sample_bpf_args[@]}"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  fi

  should_sudo="no"
  case "$sudo_mode" in
    all)
      should_sudo="yes"
      ;;
    auto)
      if should_auto_sudo "$lock_name" "$lock_script"; then
        should_sudo="yes"
      fi
      ;;
    none)
      should_sudo="no"
      ;;
  esac

  run_cmd=("${cmd[@]}")
  if [[ "$should_sudo" == "yes" ]]; then
    run_cmd=(sudo -- "${cmd[@]}")
  fi

  if [[ "$lock_kind" == "mcs_accordin_direct" && "$dry_run" != "1" ]]; then
    ensure_accordin_sched_ext_ready "$should_sudo" "$mcs_tas_accordin_sched_ext_conflict"
  elif [[ "$lock_kind" == "mcs_tas_accordin_direct" && "$dry_run" != "1" ]]; then
    ensure_accordin_sched_ext_ready "$should_sudo" "$mcs_tas_accordin_sched_ext_conflict"
  elif [[ "$lock_kind" == "ttas_accordin" && "$dry_run" != "1" ]]; then
    ensure_accordin_sched_ext_ready "$should_sudo" "$mcs_tas_accordin_sched_ext_conflict"
  elif [[ "$lock_kind" == "reciprocating_accordin" && "$dry_run" != "1" ]]; then
    ensure_accordin_sched_ext_ready "$should_sudo" "$mcs_tas_accordin_sched_ext_conflict"
  fi

  if [[ "$lock_kind" == "native" ]]; then
    echo "=== lock=${lock_name} kind=${lock_kind} bench_lock_kind=${bench_lock_kind} sudo=${should_sudo} ===" >&2
  else
    echo "=== lock=${lock_name} kind=${lock_kind} sudo=${should_sudo} ===" >&2
  fi
  printf 'Command:' >&2
  printf ' %q' "${run_cmd[@]}" >&2
  printf '\n' >&2

  if [[ "$dry_run" == "1" ]]; then
    continue
  fi

  if [[ "$with_scx_lavd" == "1" && "$scx_lavd_started" != "1" ]]; then
    start_scx_lavd "$scx_lavd_bin"
  fi

  # Pre-create outputs as invoking user so post-run files stay user-editable.
  : > "$raw_out"
  : > "$summary_out"
  chmod u+rw "$raw_out" "$summary_out"

  run_command_with_optional_bpf_profile \
    "$lock_name" "$lock_dir" "$should_sudo" "$lock_bpf_profile_enabled" \
    "${run_cmd[@]}"
  generate_perf_reports_for_lock "$lock_name" "$raw_out" "$lock_dir"
  generate_bpf_profile_summary_for_lock "$lock_name" "$lock_dir"
done
