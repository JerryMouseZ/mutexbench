#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd -- "${MUTEXBENCH_DIR}/../.." && pwd)"
if [[ -n "${FLEXGUARD_DIR:-}" ]]; then
  FLEXGUARD_DIR="$(cd -- "$FLEXGUARD_DIR" && pwd)"
elif [[ -d "${MUTEXBENCH_DIR}/../flexguard" ]]; then
  FLEXGUARD_DIR="$(cd -- "${MUTEXBENCH_DIR}/../flexguard" && pwd)"
else
  FLEXGUARD_DIR=""
fi

usage() {
  cat <<'EOF'
Run mutex throughput sweep for multiple lock modes (native mutex + interpose scripts).

Usage:
  scripts/sweep_mutex_throughput_multi_lock.sh --locks CSV [options] [sweep args...]

Options:
  --locks CSV                Required. Comma-separated lock scripts.
                             Item format:
                               1) mutex or native-mutex (run native mutex; no interpose hook)
                               2) /path/to/interpose_mcs.sh
                               3) mcs=/path/to/interpose_custom.sh
                               4) mcs (resolved as $FLEXGUARD_DIR/build/interpose_mcs.sh)
                               5) lb_simple (run benchmark with LD_PRELOAD=liblb_simple.so)
                               6) native:<kind> where <kind> is one of
                                  mutex|reciprocating|hapax|mcs|twa
                             For lb_simple library path:
                               1) use $LB_SIMPLE_LIB if set
                               2) else <repo>/target/release/liblb_simple.so
                               3) else <repo>/target/debug/liblb_simple.so
  --sweep-script PATH        Sweep script to run (default: <mutexbench>/scripts/sweep_mutex_throughput.sh)
  --output-root DIR          Output root directory (default: <mutexbench>/results)
  --sudo-mode MODE           MODE in {all,auto,none} (default: all)
                             all: sudo for every lock run
                             auto: sudo only for flexguard*/hybridlock*/lb_simple locks
                             none: never sudo
  --dry-run                  Print commands only, do not execute
  -h, --help                 Show this help

All unknown args are forwarded to sweep script.
Do not pass --output-raw / --output-summary; they are generated per lock:
  <output-root>/<lock>/raw.csv
  <output-root>/<lock>/summary.csv

Example:
  scripts/sweep_mutex_throughput_multi_lock.sh \
    --locks mutex,mcs,ticket \
    --sudo-mode all \
    --threads 1,2,4,8,16 \
    --critical-iters 10,100,500 \
    --outside-iters 10,100,500 \
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

resolve_lb_simple_lib_path() {
  local path="${LB_SIMPLE_LIB:-}"
  local default_release="$PROJECT_ROOT/target/release/liblb_simple.so"
  local default_debug="$PROJECT_ROOT/target/debug/liblb_simple.so"

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

should_auto_sudo() {
  local lock_name="$1"
  local lock_script="${2:-}"

  case "$lock_name" in
    flexguard*|hybridlock*|lb_simple)
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

locks_csv=""
sweep_script="$SCRIPT_DIR/sweep_mutex_throughput.sh"
output_root="$MUTEXBENCH_DIR/results"
sudo_mode="all"
dry_run="0"
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
if [[ "$sudo_mode" != "all" && "$sudo_mode" != "auto" && "$sudo_mode" != "none" ]]; then
  echo "--sudo-mode must be one of: all, auto, none" >&2
  exit 1
fi

sweep_script="$(resolve_executable_path "$sweep_script" "$MUTEXBENCH_DIR")"
if [[ ! -x "$sweep_script" ]]; then
  echo "Sweep script is not executable: $sweep_script" >&2
  exit 1
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
  lb_simple_lib=""
  if [[ "$item" == *=* ]]; then
    lock_name="${item%%=*}"
    lock_script="${item#*=}"
    lock_name="$(trim_spaces "$lock_name")"
    lock_script="$(trim_spaces "$lock_script")"
  else
    case "$item" in
      mutex|native-mutex)
        lock_kind="native"
        lock_name="mutex"
        bench_lock_kind="mutex"
        lock_script=""
        ;;
      native:*)
        lock_kind="native"
        lock_script=""
        bench_lock_kind="${item#native:}"
        case "$bench_lock_kind" in
          mutex|reciprocating|hapax|mcs|twa)
            lock_name="$bench_lock_kind"
            ;;
          *)
            echo "Invalid native lock kind in item: $item" >&2
            exit 1
            ;;
        esac
        ;;
      lb_simple)
        lock_kind="lb_simple"
        lock_name="lb_simple"
        lock_script=""
        ;;
      *)
        lock_script="$item"
        lock_base="$(basename "$lock_script")"
        lock_name="${lock_base%.sh}"
        lock_name="${lock_name#interpose_}"
        ;;
    esac
  fi

  if [[ "$lock_kind" == "hook" && "$lock_script" != */* && "$lock_script" != *.sh && -n "${FLEXGUARD_DIR:-}" ]]; then
    candidate_script="$FLEXGUARD_DIR/build/interpose_${lock_script}.sh"
    if [[ -x "$candidate_script" ]]; then
      lock_script="$candidate_script"
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
  elif [[ "$lock_kind" == "lb_simple" ]]; then
    lb_simple_lib="$(resolve_lb_simple_lib_path)"
    if [[ ! -f "$lb_simple_lib" ]]; then
      echo "lb_simple library not found: $lb_simple_lib" >&2
      echo "Build first (cargo build --release) or set LB_SIMPLE_LIB to liblb_simple.so path." >&2
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
  elif [[ "$lock_kind" == "lb_simple" ]]; then
    cmd=(
      "$sweep_script"
      "${sweep_args[@]}"
      --bench-ld-preload "$lb_simple_lib"
      --output-raw "$raw_out"
      --output-summary "$summary_out"
    )
  else
    cmd=(
      "$lock_script"
      "$sweep_script"
      "${sweep_args[@]}"
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

  # Pre-create outputs as invoking user so post-run files stay user-editable.
  : > "$raw_out"
  : > "$summary_out"
  chmod u+rw "$raw_out" "$summary_out"

  "${run_cmd[@]}"
done
