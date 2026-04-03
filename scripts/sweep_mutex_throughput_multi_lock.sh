#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd -- "${MUTEXBENCH_DIR}/../.." && pwd)"
LB_SIMPLE_DEBUG_COUNTERS="${LB_SIMPLE_DEBUG_COUNTERS:-}"
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
                               1) builtin lock kind (native run): mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh
                               2) native-mutex (alias of native:mutex)
                               3) native:<kind> where <kind> is one of
                                  mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh
                               4) /path/to/interpose_mcs.sh
                               5) mcs=/path/to/interpose_custom.sh
                               6) non-builtin short name (e.g. flexguard),
                                  resolved as $FLEXGUARD_DIR/build/interpose_<name>.sh
                               7) mcs_tas_simple (run benchmark with LD_PRELOAD=libmcs_tas_simple.so)
                               8) mcs_tas_simple_no_bpf (same as mcs_tas_simple with MCS_TAS_SIMPLE_DISABLE_BPF=1)
                               9) flexguard_simple (run benchmark with LD_PRELOAD=libflexguard.so)
                             Name conflict rule:
                               - Builtin names always run as native locks.
                               - To run external lock with a builtin-like name,
                                 use explicit script path or name=/path/to/script.sh
                             For mcs_tas_simple library path:
                               1) use $MCS_TAS_SIMPLE_LIB if set
                               2) else <repo>/target/release/libmcs_tas_simple.so
                               3) else <repo>/target/debug/libmcs_tas_simple.so
                             For flexguard_simple library path:
                               1) use $FLEXGUARD_SIMPLE_LIB if set
                               2) else <repo>/target/release/libflexguard.so
                               3) else <repo>/target/debug/libflexguard.so
  --sweep-script PATH        Sweep script to run (default: <mutexbench>/scripts/sweep_mutex_throughput.sh)
  --output-root DIR          Output root directory (default: <mutexbench>/results)
  --sudo-mode MODE           MODE in {all,auto,none} (default: all)
                             all: sudo for every lock run
                             auto: sudo only for flexguard*/hybridlock*/mcs_tas_simple* locks
                             none: never sudo
  --timeslice-extension M    off|auto|require (default: off)
  --with-scx-lavd            Run scx_lavd in background for the whole sweep:
                               sudo /mnt/home/jz/scx/target/release/scx_lavd --per-cpu-dsq --performance
                             It will be stopped after all locks finish.
  --scx-lavd-bin PATH        scx_lavd binary path (default: /mnt/home/jz/scx/target/release/scx_lavd)
  --lb-simple-sched-ext-conflict MODE
                             MODE in {stop,error,ignore} (default: stop)
                             How to handle active sched_ext before mcs_tas_simple:
                               stop: terminate current sched_ext owner process(es)
                               error: fail fast with owner diagnostics
                               ignore: run anyway (mcs_tas_simple may fail to initialize)
  --dry-run                  Print commands only, do not execute
  -h, --help                 Show this help

All unknown args are forwarded to sweep script.
This script serializes globally with flock so concurrent invocations queue.
Default queue lock file: /tmp/mutexbench-sweep-multi-lock.lock
Override queue lock file with env: MUTEXBENCH_MULTI_LOCK_LOCK_FILE=/path/to/lock
Do not pass --output-raw / --output-summary; they are generated per lock:
  <output-root>/<lock>/raw.csv
  <output-root>/<lock>/summary.csv

Example:
  scripts/sweep_mutex_throughput_multi_lock.sh \
    --locks mutex,mcs,ticket \
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

resolve_mcs_tas_simple_lib_path() {
  resolve_preload_lib_path \
    "MCS_TAS_SIMPLE_LIB" \
    "$PROJECT_ROOT/target/release/libmcs_tas_simple.so" \
    "$PROJECT_ROOT/target/debug/libmcs_tas_simple.so"
}

resolve_flexguard_simple_lib_path() {
  resolve_preload_lib_path \
    "FLEXGUARD_SIMPLE_LIB" \
    "$PROJECT_ROOT/target/release/libflexguard.so" \
    "$PROJECT_ROOT/target/debug/libflexguard.so"
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
    mutex|reciprocating|hapax|mcs|mcs-tas|mcs-tas-tse|mcstas-next|mcstas-next-tse|twa|clh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
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

ensure_mcs_tas_simple_sched_ext_ready() {
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
    echo "mcs_tas_simple requires exclusive sched_ext, but current state is enabled (ops=${ops:-unknown})." >&2
    echo "Current owner(s): ${owners}" >&2
    echo "Stop active scheduler first, or use --lb-simple-sched-ext-conflict stop." >&2
    return 1
  fi

  if ! command -v bpftool >/dev/null 2>&1; then
    echo "sched_ext is enabled (ops=${ops:-unknown}), but bpftool is unavailable; cannot auto-stop owner process." >&2
    return 1
  fi
  if [[ "$use_sudo" != "yes" && "$EUID" -ne 0 ]]; then
    echo "sched_ext is enabled (ops=${ops:-unknown}) and stopping it requires root." >&2
    echo "Use --sudo-mode all/auto, run script under sudo, or switch to --lb-simple-sched-ext-conflict error." >&2
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

  echo "[mcs_tas_simple] Active sched_ext detected (ops=${ops:-unknown}); stopping owner PIDs: ${owner_pids[*]}" >&2
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
    echo "[mcs_tas_simple] sched_ext still enabled after SIGTERM; sending SIGKILL to: ${owner_pids[*]}" >&2
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
    echo "Failed to clear active sched_ext (ops=${ops:-unknown}); mcs_tas_simple cannot start." >&2
    return 1
  fi

  return 0
}

should_auto_sudo() {
  local lock_name="$1"
  local lock_script="${2:-}"

  case "$lock_name" in
    flexguard*|hybridlock*|mcs_tas_simple*)
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
sudo_mode="all"
timeslice_extension="off"
with_scx_lavd="0"
scx_lavd_bin="/mnt/home/jz/scx/target/release/scx_lavd"
mcs_tas_simple_sched_ext_conflict="stop"
dry_run="0"
queue_lock_file="${MUTEXBENCH_MULTI_LOCK_LOCK_FILE:-/tmp/mutexbench-sweep-multi-lock.lock}"
declare -a sweep_args=()

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
    --sudo-mode)
      sudo_mode="${2:-}"
      shift 2
      ;;
    --timeslice-extension)
      timeslice_extension="${2:-}"
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
    --lb-simple-sched-ext-conflict)
      mcs_tas_simple_sched_ext_conflict="${2:-}"
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
if contains_flag "--timeslice-extension" "${sweep_args[@]}"; then
  echo "Do not pass --timeslice-extension through forwarded sweep args. Use the top-level flag instead." >&2
  exit 1
fi
if [[ "$sudo_mode" != "all" && "$sudo_mode" != "auto" && "$sudo_mode" != "none" ]]; then
  echo "--sudo-mode must be one of: all, auto, none" >&2
  exit 1
fi
if [[ "$timeslice_extension" != "off" && "$timeslice_extension" != "auto" && "$timeslice_extension" != "require" ]]; then
  echo "--timeslice-extension must be one of: off, auto, require" >&2
  exit 1
fi
if [[ "$mcs_tas_simple_sched_ext_conflict" != "stop" && "$mcs_tas_simple_sched_ext_conflict" != "error" && "$mcs_tas_simple_sched_ext_conflict" != "ignore" ]]; then
  echo "--lb-simple-sched-ext-conflict must be one of: stop, error, ignore" >&2
  exit 1
fi

queue_lock_file="$(expand_home "$queue_lock_file")"
if [[ "$queue_lock_file" != /* ]]; then
  queue_lock_file="$MUTEXBENCH_DIR/$queue_lock_file"
fi
acquire_queue_lock "$queue_lock_file"

sweep_args+=(--timeslice-extension "$timeslice_extension")

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
  mcs_tas_simple_lib=""
  flexguard_simple_lib=""
  mcs_tas_simple_disable_bpf="0"
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
      mcs_tas_simple)
        lock_kind="mcs_tas_simple"
        lock_name="mcs_tas_simple"
        lock_script=""
        ;;
      mcs_tas_simple_no_bpf)
        lock_kind="mcs_tas_simple"
        lock_name="mcs_tas_simple_no_bpf"
        lock_script=""
        mcs_tas_simple_disable_bpf="1"
        ;;
      flexguard_simple)
        lock_kind="flexguard_simple"
        lock_name="flexguard_simple"
        lock_script=""
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
  elif [[ "$lock_kind" == "mcs_tas_simple" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    mcs_tas_simple_lib="$(resolve_mcs_tas_simple_lib_path)"
    if [[ ! -f "$mcs_tas_simple_lib" ]]; then
      echo "mcs_tas_simple library not found: $mcs_tas_simple_lib" >&2
      echo "Build first (cargo build -p mcs_tas_simple --release) or set LB_SIMPLE_LIB to libmcs_tas_simple.so path." >&2
      exit 1
    fi
  elif [[ "$lock_kind" == "flexguard_simple" ]]; then
    if [[ "$with_scx_lavd" == "1" ]]; then
      echo "lock=${lock_name} cannot be used together with --with-scx-lavd (both need sched_ext ownership)." >&2
      exit 1
    fi
    flexguard_simple_lib="$(resolve_flexguard_simple_lib_path)"
    if [[ ! -f "$flexguard_simple_lib" ]]; then
      echo "flexguard_simple library not found: $flexguard_simple_lib" >&2
      echo "Build first (cargo build -p libflexguard --release) or set FLEXGUARD_SIMPLE_LIB to libflexguard.so path." >&2
      exit 1
    fi
  fi

  lock_dir="${output_root}/${lock_name}"
  raw_out="${lock_dir}/raw.csv"
  summary_out="${lock_dir}/summary.csv"
  mkdir -p "$lock_dir"

  if [[ "$lock_kind" == "native" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --lock-kind "$bench_lock_kind"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  elif [[ "$lock_kind" == "mcs_tas_simple" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --bench-ld-preload "$mcs_tas_simple_lib"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
    if [[ "$mcs_tas_simple_disable_bpf" == "1" ]]; then
      cmd=(env "LB_SIMPLE_DEBUG_COUNTERS=${LB_SIMPLE_DEBUG_COUNTERS}" "LB_SIMPLE_DISABLE_BPF=1" "${cmd[@]}")
    else
      cmd=(env "LB_SIMPLE_DEBUG_COUNTERS=${LB_SIMPLE_DEBUG_COUNTERS}" "${cmd[@]}")
    fi
  elif [[ "$lock_kind" == "flexguard_simple" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --bench-ld-preload "$flexguard_simple_lib"
      --lock-kind "mutex"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  else
    cmd=(
      "$lock_script"
      "$sweep_script"
      "${sweep_args[@]}"
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

  if [[ "$lock_kind" == "mcs_tas_simple" && "$dry_run" != "1" ]]; then
    ensure_mcs_tas_simple_sched_ext_ready "$should_sudo" "$mcs_tas_simple_sched_ext_conflict"
  elif [[ "$lock_kind" == "flexguard_simple" && "$dry_run" != "1" ]]; then
    ensure_mcs_tas_simple_sched_ext_ready "$should_sudo" "$mcs_tas_simple_sched_ext_conflict"
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

  "${run_cmd[@]}"
done
