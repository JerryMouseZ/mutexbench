#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

from bench_csv_schema import (
    RAW_FIELDNAMES,
    SUMMARY_FIELDNAMES,
    aggregate_summary_rows,
    normalize_raw_rows,
    read_csv_rows,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Normalize mutexbench raw.csv/summary.csv to the current schema."
    )
    p.add_argument(
        "roots",
        nargs="*",
        default=["results", "results_tse"],
        help="Result roots that contain <lock>/raw.csv and <lock>/summary.csv",
    )
    return p.parse_args()


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def migrate_lock_dir(lock_dir: Path) -> tuple[int, int]:
    raw_path = lock_dir / "raw.csv"
    summary_path = lock_dir / "summary.csv"
    if not raw_path.is_file():
        raise ValueError(f"{lock_dir}: missing raw.csv")

    raw_fieldnames, raw_rows = read_csv_rows(raw_path)
    normalized_raw = normalize_raw_rows(raw_fieldnames, raw_rows, raw_path)
    summary_rows = aggregate_summary_rows(normalized_raw)

    write_csv(raw_path, RAW_FIELDNAMES, normalized_raw)
    write_csv(summary_path, SUMMARY_FIELDNAMES, summary_rows)
    return len(normalized_raw), len(summary_rows)


def iter_lock_dirs(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(
        child
        for child in root.iterdir()
        if child.is_dir()
        and ((child / "raw.csv").is_file() or (child / "summary.csv").is_file())
    )


def main() -> None:
    args = parse_args()
    migrated = 0
    for raw_root in args.roots:
        root = Path(raw_root)
        lock_dirs = iter_lock_dirs(root)
        if not lock_dirs:
            print(f"[skip] {root}: no result directories")
            continue
        for lock_dir in lock_dirs:
            raw_count, summary_count = migrate_lock_dir(lock_dir)
            migrated += 1
            print(f"[ok] {lock_dir}: raw_rows={raw_count} summary_rows={summary_count}")
    if migrated == 0:
        raise SystemExit("no datasets migrated")


if __name__ == "__main__":
    main()
