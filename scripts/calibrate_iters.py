#!/usr/bin/env python3
"""Calibrate BurnIters and optionally write iter_calibration.cfg."""

from __future__ import annotations

import argparse
import csv
import math
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from fractions import Fraction
from pathlib import Path
from typing import Sequence


SCRIPT_DIR = Path(__file__).resolve().parent
MUTEXBENCH_DIR = SCRIPT_DIR.parent


@dataclass(frozen=True)
class Calibration:
    numerator: int
    denominator: int

    @property
    def ratio(self) -> float:
        return self.numerator / self.denominator

    @property
    def display(self) -> str:
        return f"{self.numerator}/{self.denominator}"


@dataclass(frozen=True)
class Point:
    value: int
    value_ns: float
    extra: dict[str, str]


@dataclass(frozen=True)
class FitResult:
    slope_ns_per_unit: float
    intercept_ns: float
    r_squared: float
    points_used: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Measure BurnIters cost, suggest a calibration ratio, and optionally "
            "write iter_calibration.cfg."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("curve", "mutex"),
        default="curve",
        help="Measurement path: curve_bench or mutex_bench (default: curve)",
    )
    parser.add_argument(
        "--binary",
        default=None,
        help="Benchmark binary path (default: ../curve_bench or ../mutex_bench)",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Source file path used to read fallback calibration constants",
    )
    parser.add_argument(
        "--calibration-config",
        default=None,
        help=(
            "Calibration config path used for runtime measurement and optional "
            "write-back (default: <binary-dir>/iter_calibration.cfg)"
        ),
    )
    parser.add_argument(
        "--write-config",
        action="store_true",
        help="Write suggested calibration into --calibration-config",
    )
    parser.add_argument(
        "--current-calibration",
        default=None,
        help="Override current effective calibration as NUM/DEN",
    )
    parser.add_argument(
        "--target-ns-per-iter",
        type=float,
        default=1.0,
        help="Desired ns per logical workload unit (default: 1.0)",
    )
    parser.add_argument(
        "--min-iters",
        type=int,
        default=0,
        help="Minimum logical workload value to measure (default: 0)",
    )
    parser.add_argument(
        "--max-iters",
        type=int,
        default=4000,
        help="Maximum logical workload value to measure (default: 4000)",
    )
    parser.add_argument(
        "--step-iters",
        type=int,
        default=200,
        help="Step between measured workload values (default: 200)",
    )
    parser.add_argument(
        "--fit-min-iters",
        type=int,
        default=None,
        help="Ignore measured points below this workload value when fitting",
    )
    parser.add_argument(
        "--suggest-denominator",
        type=int,
        default=None,
        help="Denominator for suggested calibration (default: current denominator or 32)",
    )
    parser.add_argument(
        "--map-ns",
        "--map-iters",
        default=None,
        help="Comma-separated workload values to remap to the target calibration",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Optional path to write measured points",
    )
    parser.add_argument(
        "--batch",
        type=int,
        default=20000,
        help="curve mode: calls per timing batch (default: 20000)",
    )
    parser.add_argument(
        "--repeats",
        type=int,
        default=20,
        help="curve mode: timed batches per point (default: 20)",
    )
    parser.add_argument(
        "--warmup-batches",
        type=int,
        default=5,
        help="curve mode: warmup batches before timing (default: 5)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=1,
        help="mutex mode: worker threads (default: 1)",
    )
    parser.add_argument(
        "--outside-ns",
        "--outside-iters",
        type=int,
        default=0,
        help="mutex mode: fixed non-critical-section workload in ns (default: 0)",
    )
    parser.add_argument(
        "--duration-ms",
        type=int,
        default=1200,
        help="mutex mode: measurement duration in ms (default: 1200)",
    )
    parser.add_argument(
        "--warmup-duration-ms",
        type=int,
        default=100,
        help="mutex mode: warmup duration in ms (default: 100)",
    )
    parser.add_argument(
        "--timing-sample-stride",
        type=int,
        default=1,
        help="mutex mode: timing sample stride (default: 1)",
    )
    parser.add_argument(
        "--lock-kind",
        default="mutex",
        help="mutex mode: lock kind (default: mutex)",
    )
    parser.add_argument(
        "--timeslice-extension",
        choices=("off", "auto", "require"),
        default="off",
        help="mutex mode: timeslice extension mode (default: off)",
    )
    return parser.parse_args()


def parse_calibration(spec: str) -> Calibration:
    match = re.fullmatch(r"\s*(\d+)\s*/\s*(\d+)\s*", spec)
    if not match:
        raise ValueError(f"Invalid calibration value: {spec!r} (expected NUM/DEN)")
    numerator = int(match.group(1))
    denominator = int(match.group(2))
    if denominator <= 0:
        raise ValueError("Calibration denominator must be > 0")
    return Calibration(numerator=numerator, denominator=denominator)


def read_source_calibration(source_path: Path | None) -> Calibration | None:
    if source_path is None or not source_path.is_file():
        return None
    text = source_path.read_text(encoding="utf-8")
    numerator_match = re.search(
        r"k(?:Default)?BurnCalibrationNumerator\s*=\s*(\d+)", text
    )
    denominator_match = re.search(
        r"k(?:Default)?BurnCalibrationDenominator\s*=\s*(\d+)", text
    )
    if not numerator_match or not denominator_match:
        return None
    return Calibration(
        numerator=int(numerator_match.group(1)),
        denominator=int(denominator_match.group(1)),
    )


def default_binary(mode: str) -> Path:
    if mode == "curve":
        return MUTEXBENCH_DIR / "curve_bench"
    return MUTEXBENCH_DIR / "mutex_bench"


def default_source(mode: str) -> Path:
    if mode == "curve":
        return MUTEXBENCH_DIR / "curve_bench.cpp"
    return MUTEXBENCH_DIR / "mutex_bench.cpp"


def default_calibration_config(binary: Path) -> Path:
    return binary.resolve().parent / "iter_calibration.cfg"


def resolve_optional_path(value: str | None, fallback: Path) -> Path:
    if value is None:
        return fallback
    return Path(value).expanduser().resolve()


def ensure_positive(value: int, name: str) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0")


def measure_values(min_value: int, max_value: int, step_value: int) -> list[int]:
    if min_value < 0:
        raise ValueError("--min-iters must be >= 0")
    if max_value < min_value:
        raise ValueError("--max-iters must be >= --min-iters")
    ensure_positive(step_value, "--step-iters")

    values: list[int] = []
    current = min_value
    while current <= max_value:
        values.append(current)
        if max_value - current < step_value:
            break
        current += step_value
    if len(values) < 2:
        raise ValueError("Need at least two measurement points")
    return values


def run_command(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def config_section_name(mode: str) -> str:
    return "curve_bench" if mode == "curve" else "mutex_bench"


def read_config_entries(path: Path) -> dict[str, str]:
    entries: dict[str, str] = {}
    if not path.is_file():
        return entries
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid config line {line_number} in {path}")
        key, value = line.split("=", 1)
        entries[key.strip()] = value.strip()
    return entries


def read_config_calibration(path: Path, mode: str) -> Calibration | None:
    section = config_section_name(mode)
    entries = read_config_entries(path)
    numerator = entries.get(f"{section}.numerator")
    denominator = entries.get(f"{section}.denominator")
    if numerator is None and denominator is None:
        return None
    if numerator is None or denominator is None:
        raise ValueError(f"Incomplete calibration section {section!r} in {path}")
    return Calibration(numerator=int(numerator), denominator=int(denominator))


def measure_curve(
    args: argparse.Namespace, binary: Path, runtime_config: Path | None
) -> list[Point]:
    ensure_positive(args.batch, "--batch")
    ensure_positive(args.repeats, "--repeats")
    ensure_positive(args.warmup_batches, "--warmup-batches")

    command = [
        str(binary),
        "--min-iters",
        str(args.min_iters),
        "--max-iters",
        str(args.max_iters),
        "--step-iters",
        str(args.step_iters),
        "--batch",
        str(args.batch),
        "--repeats",
        str(args.repeats),
        "--warmup-batches",
        str(args.warmup_batches),
    ]
    if runtime_config is not None:
        command.extend(["--calibration-config", str(runtime_config)])
    completed = run_command(command)
    if completed.stderr:
        print(completed.stderr.rstrip(), file=sys.stderr)

    rows = csv.DictReader(completed.stdout.splitlines())
    points: list[Point] = []
    for row in rows:
        points.append(
            Point(
                value=int(row["iters"]),
                value_ns=float(row["avg_call_ns"]),
                extra=row,
            )
        )
    if len(points) < 2:
        raise ValueError("curve_bench returned fewer than two points")
    return points


def extract_metric(output: str, key: str) -> float:
    prefix = f"{key}:"
    for line in output.splitlines():
        if line.startswith(prefix):
            return float(line.split(":", 1)[1].strip())
    raise ValueError(f"Failed to find metric {key!r} in benchmark output")


def measure_mutex(
    args: argparse.Namespace, binary: Path, runtime_config: Path | None
) -> list[Point]:
    ensure_positive(args.threads, "--threads")
    ensure_positive(args.duration_ms, "--duration-ms")
    ensure_positive(args.timing_sample_stride, "--timing-sample-stride")

    points: list[Point] = []
    for value in measure_values(args.min_iters, args.max_iters, args.step_iters):
        print(
            f"Measuring mutex_bench at critical_ns={value} "
            f"(threads={args.threads}, outside_ns={args.outside_ns})",
            file=sys.stderr,
        )
        command = [
            str(binary),
            "--threads",
            str(args.threads),
            "--duration-ms",
            str(args.duration_ms),
            "--warmup-duration-ms",
            str(args.warmup_duration_ms),
            "--critical-ns",
            str(value),
            "--outside-ns",
            str(args.outside_ns),
            "--timing-sample-stride",
            str(args.timing_sample_stride),
            "--lock-kind",
            args.lock_kind,
            "--timeslice-extension",
            args.timeslice_extension,
        ]
        if runtime_config is not None:
            command.extend(["--calibration-config", str(runtime_config)])
        completed = run_command(command)
        if completed.stderr:
            print(completed.stderr.rstrip(), file=sys.stderr)
        avg_lock_hold_ns = extract_metric(completed.stdout, "avg_lock_hold_ns")
        throughput = extract_metric(completed.stdout, "throughput_ops_per_sec")
        points.append(
            Point(
                value=value,
                value_ns=avg_lock_hold_ns,
                extra={
                    "value": str(value),
                    "avg_lock_hold_ns": f"{avg_lock_hold_ns:.6f}",
                    "throughput_ops_per_sec": f"{throughput:.6f}",
                },
            )
        )
    return points


def fit_line(points: Sequence[Point], fit_min_value: int) -> FitResult:
    fit_points = [point for point in points if point.value >= fit_min_value]
    if len(fit_points) < 2:
        raise ValueError(
            f"Need at least two points at or above --fit-min-iters={fit_min_value}"
        )

    xs = [float(point.value) for point in fit_points]
    ys = [point.value_ns for point in fit_points]
    n = float(len(fit_points))
    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xx = sum(x * x for x in xs)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    denominator = n * sum_xx - sum_x * sum_x
    if math.isclose(denominator, 0.0):
        raise ValueError("Degenerate fit: workload values are not distinct")

    slope = (n * sum_xy - sum_x * sum_y) / denominator
    intercept = (sum_y - slope * sum_x) / n

    mean_y = sum_y / n
    ss_tot = sum((y - mean_y) ** 2 for y in ys)
    ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
    r_squared = 1.0 if math.isclose(ss_tot, 0.0) else 1.0 - (ss_res / ss_tot)

    return FitResult(
        slope_ns_per_unit=slope,
        intercept_ns=intercept,
        r_squared=r_squared,
        points_used=len(fit_points),
    )


def suggest_calibration(
    current: Calibration | None,
    target_ns_per_iter: float,
    observed_ns_per_iter: float,
    denominator_override: int | None,
) -> Calibration | None:
    if current is None:
        return None
    if observed_ns_per_iter <= 0.0:
        raise ValueError("Observed ns/iter must be > 0")

    denominator = denominator_override or current.denominator or 32
    if denominator <= 0:
        raise ValueError("--suggest-denominator must be > 0")

    suggested_ratio = current.ratio * (target_ns_per_iter / observed_ns_per_iter)
    numerator = max(1, int(round(suggested_ratio * denominator)))
    fraction = Fraction(numerator, denominator)
    return Calibration(
        numerator=fraction.numerator,
        denominator=fraction.denominator,
    )


def parse_mapping_values(spec: str | None) -> list[int]:
    if spec is None or not spec.strip():
        return []
    values = []
    for raw in spec.split(","):
        raw = raw.strip()
        if not raw:
            continue
        value = int(raw)
        if value < 0:
            raise ValueError("--map-ns values must be >= 0")
        values.append(value)
    return values


def write_points_csv(path: Path, points: Sequence[Point], mode: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if mode == "curve":
        fieldnames = [
            "value",
            "avg_batch_ns",
            "min_batch_ns",
            "max_batch_ns",
            "avg_call_ns",
        ]
    else:
        fieldnames = [
            "value",
            "avg_lock_hold_ns",
            "throughput_ops_per_sec",
        ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for point in points:
            row = dict(point.extra)
            row["value"] = str(point.value)
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_config_file(
    path: Path,
    mode: str,
    calibration: Calibration,
    fit: FitResult,
    target_ns_per_iter: float,
    binary: Path,
) -> None:
    section = config_section_name(mode)
    entries = read_config_entries(path)
    entries["version"] = "1"
    entries[f"{section}.numerator"] = str(calibration.numerator)
    entries[f"{section}.denominator"] = str(calibration.denominator)
    entries[f"{section}.target_ns_per_iter"] = f"{target_ns_per_iter:.6f}"
    entries[f"{section}.observed_ns_per_iter"] = f"{fit.slope_ns_per_unit:.6f}"
    entries[f"{section}.fit_intercept_ns"] = f"{fit.intercept_ns:.6f}"
    entries[f"{section}.fit_r_squared"] = f"{fit.r_squared:.6f}"
    entries[f"{section}.points_used_in_fit"] = str(fit.points_used)
    entries[f"{section}.generated_at_utc"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    entries[f"{section}.binary"] = str(binary)

    ordered_keys = ["version"]
    for prefix in ("mutex_bench", "curve_bench"):
        ordered_keys.extend(
            key for key in sorted(entries) if key.startswith(f"{prefix}.")
        )
    ordered_keys.extend(
        key
        for key in sorted(entries)
        if key != "version"
        and not key.startswith("mutex_bench.")
        and not key.startswith("curve_bench.")
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        handle.write("# Auto-generated by scripts/calibrate_iters.py\n")
        for key in ordered_keys:
            if key in entries:
                handle.write(f"{key}={entries[key]}\n")


def main() -> int:
    args = parse_args()
    binary = resolve_optional_path(args.binary, default_binary(args.mode))
    source = resolve_optional_path(args.source, default_source(args.mode))
    calibration_config = resolve_optional_path(
        args.calibration_config, default_calibration_config(binary)
    )

    if not binary.is_file():
        print(f"Benchmark binary not found: {binary}", file=sys.stderr)
        return 1
    if not binary.stat().st_mode & 0o111:
        print(f"Benchmark binary is not executable: {binary}", file=sys.stderr)
        return 1
    if args.target_ns_per_iter <= 0.0:
        print("--target-ns-per-iter must be > 0", file=sys.stderr)
        return 1

    try:
        config_calibration = read_config_calibration(calibration_config, args.mode)
        source_calibration = read_source_calibration(source)
        calibration = (
            parse_calibration(args.current_calibration)
            if args.current_calibration
            else config_calibration or source_calibration
        )
        runtime_config = calibration_config if config_calibration is not None else None

        if args.mode == "curve":
            points = measure_curve(args, binary, runtime_config)
        else:
            points = measure_mutex(args, binary, runtime_config)

        fit_min_iters = args.fit_min_iters
        if fit_min_iters is None:
            fit_min_iters = max(args.min_iters, max(args.step_iters, args.max_iters // 10))

        fit = fit_line(points, fit_min_value=fit_min_iters)
        if fit.slope_ns_per_unit <= 0.0:
            raise ValueError(
                f"Fitted slope must be > 0, got {fit.slope_ns_per_unit:.6f}"
            )

        suggested = suggest_calibration(
            current=calibration,
            target_ns_per_iter=args.target_ns_per_iter,
            observed_ns_per_iter=fit.slope_ns_per_unit,
            denominator_override=args.suggest_denominator,
        )
        cli_multiplier = args.target_ns_per_iter / fit.slope_ns_per_unit
        mapped_values = parse_mapping_values(args.map_ns)

        if args.output_csv:
            output_csv = Path(args.output_csv).expanduser().resolve()
            write_points_csv(output_csv, points, args.mode)
        if args.write_config:
            if suggested is None:
                raise ValueError("Cannot write config without a known calibration base")
            write_config_file(
                path=calibration_config,
                mode=args.mode,
                calibration=suggested,
                fit=fit,
                target_ns_per_iter=args.target_ns_per_iter,
                binary=binary,
            )

        print(f"mode: {args.mode}")
        print(f"binary: {binary}")
        print(f"source: {source}")
        print(f"calibration_config: {calibration_config}")
        print(f"target_ns_per_iter: {args.target_ns_per_iter:.6f}")
        print(
            f"fit_window_values: [{fit_min_iters}, {max(point.value for point in points)}]"
        )
        print(f"points_measured: {len(points)}")
        print(f"points_used_in_fit: {fit.points_used}")
        print(f"observed_ns_per_iter: {fit.slope_ns_per_unit:.6f}")
        print(f"fit_intercept_ns: {fit.intercept_ns:.6f}")
        print(f"fit_r_squared: {fit.r_squared:.6f}")
        print(f"cli_value_multiplier: {cli_multiplier:.6f}")
        if calibration is not None:
            print(f"current_effective_calibration: {calibration.display}")
        else:
            print("current_effective_calibration: unavailable")
        if suggested is not None:
            print(f"suggested_source_calibration: {suggested.display}")
        else:
            print("suggested_source_calibration: unavailable")

        if mapped_values:
            print("mapped_values:")
            for value in mapped_values:
                mapped = int(round(value * cli_multiplier))
                print(f"  {value} -> {mapped}")

        if args.output_csv:
            print(f"measurement_csv: {output_csv}")
        if args.write_config and suggested is not None:
            print(f"written_calibration_config: {calibration_config}")
    except (OSError, subprocess.CalledProcessError, ValueError) as exc:
        if isinstance(exc, subprocess.CalledProcessError):
            if exc.stderr:
                print(exc.stderr.rstrip(), file=sys.stderr)
            if exc.stdout:
                print(exc.stdout.rstrip(), file=sys.stderr)
            print(f"Command failed: {' '.join(exc.cmd)}", file=sys.stderr)
        else:
            print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
