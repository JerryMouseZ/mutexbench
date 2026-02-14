#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MUTEXBENCH_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
if [[ -n "${FLEXGUARD_DIR:-}" ]]; then
  FLEXGUARD_DIR="$(cd -- "$FLEXGUARD_DIR" && pwd)"
elif [[ -d "${MUTEXBENCH_DIR}/../flexguard" ]]; then
  FLEXGUARD_DIR="$(cd -- "${MUTEXBENCH_DIR}/../flexguard" && pwd)"
else
  FLEXGUARD_DIR=""
fi

usage() {
  cat <<'EOF'
Run mutex throughput sweep for multiple lock interpose scripts.

Usage:
  scripts/sweep_mutex_throughput_multi_lock.sh --locks CSV [options] [sweep args...]

Options:
  --locks CSV                Required. Comma-separated lock scripts.
                             Item format:
                               1) /path/to/interpose_mcs.sh
                               2) mcs=/path/to/interpose_custom.sh
                               3) mcs (resolved as $FLEXGUARD_DIR/build/interpose_mcs.sh)
  --sweep-script PATH        Sweep script to run (default: <mutexbench>/scripts/sweep_mutex_throughput.sh)
  --output-root DIR          Output root directory (default: <mutexbench>/results)
  --dry-run                  Print commands only, do not execute
  -h, --help                 Show this help

All unknown args are forwarded to sweep script.
Do not pass --output-raw / --output-summary; they are generated per lock:
  <output-root>/<lock>/raw.csv
  <output-root>/<lock>/summary.csv

Example:
  scripts/sweep_mutex_throughput_multi_lock.sh \
    --locks mcs,ticket \
    --threads 1,2,4,8,16 \
    --critical-iters 10,100,500 \
    --outside-iters 10,100,500 \
    --iterations 300000 \
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

locks_csv=""
sweep_script="$SCRIPT_DIR/sweep_mutex_throughput.sh"
output_root="$MUTEXBENCH_DIR/results"
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
  if [[ "$item" == *=* ]]; then
    lock_name="${item%%=*}"
    lock_script="${item#*=}"
    lock_name="$(trim_spaces "$lock_name")"
    lock_script="$(trim_spaces "$lock_script")"
  else
    lock_script="$item"
    lock_base="$(basename "$lock_script")"
    lock_name="${lock_base%.sh}"
    lock_name="${lock_name#interpose_}"
  fi

  if [[ "$lock_script" != */* && "$lock_script" != *.sh && -n "${FLEXGUARD_DIR:-}" ]]; then
    candidate_script="$FLEXGUARD_DIR/build/interpose_${lock_script}.sh"
    if [[ -x "$candidate_script" ]]; then
      lock_script="$candidate_script"
    fi
  fi

  if [[ -z "$lock_name" || -z "$lock_script" ]]; then
    echo "Invalid lock item: $item" >&2
    exit 1
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

  lock_script="$(resolve_executable_path "$lock_script" "$MUTEXBENCH_DIR")"
  if [[ ! -x "$lock_script" ]]; then
    echo "Lock script is not executable: $lock_script" >&2
    exit 1
  fi

  lock_dir="${output_root}/${lock_name}"
  raw_out="${lock_dir}/raw.csv"
  summary_out="${lock_dir}/summary.csv"
  mkdir -p "$lock_dir"

  cmd=(
    "$lock_script"
    "$sweep_script"
    "${sweep_args[@]}"
    --output-raw "$raw_out"
    --output-summary "$summary_out"
  )

  echo "=== lock=${lock_name} ===" >&2
  printf 'Command:' >&2
  printf ' %q' "${cmd[@]}" >&2
  printf '\n' >&2

  if [[ "$dry_run" == "1" ]]; then
    continue
  fi

  "${cmd[@]}"
done
