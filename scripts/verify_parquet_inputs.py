"""Generate and verify parquet inputs for graph-development experiments.

Usage:
    uv run python scripts/verify_parquet_inputs.py

This script can also validate an existing output root without regenerating it:
    uv run python scripts/verify_parquet_inputs.py --skip-generate
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pandas as pd

from hydrology_graphs.io.parquet_store import scan_parquet_catalog
from river_meta.rainfall.storage.parquet_store import load_records_parquet, scan_parquet_dir


EXPECTED_COLUMNS = [
    "source",
    "station_key",
    "station_name",
    "period_start_at",
    "period_end_at",
    "observed_at",
    "metric",
    "value",
    "unit",
    "interval",
    "quality",
]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _script_path(name: str) -> Path:
    return Path(__file__).resolve().with_name(name)


def _run(cmd: list[str], *, cwd: Path, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _generate_parquets(repo: Path, output_root: Path, temp_root: Path, seed: int | None) -> tuple[int, dict[str, Any]]:
    cmd = [
        sys.executable,
        str(_script_path("generate_parquet_input_samples.py")),
        "--output-root",
        str(output_root),
        "--temp-root",
        str(temp_root),
    ]
    if seed is not None:
        cmd.extend(["--seed", str(seed)])
    proc = _run(cmd, cwd=repo, timeout=1_200)
    payload: dict[str, Any] = {
        "returncode": proc.returncode,
        "command": cmd,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
    return proc.returncode, payload


def _parse_station_name_from_parquet(parquet_path: Path) -> str:
    df = pd.read_parquet(parquet_path, columns=["station_name"])
    if df.empty or "station_name" not in df.columns:
        return ""
    return str(df["station_name"].iloc[0]).strip()


def _summarize_parquet(path: Path) -> dict[str, Any]:
    raw = pd.read_parquet(path)
    compat = load_records_parquet(path)
    raw_columns = list(raw.columns)
    missing_expected = [column for column in EXPECTED_COLUMNS if column not in raw_columns]
    extra_columns = [column for column in raw_columns if column not in EXPECTED_COLUMNS]
    return {
        "path": str(path),
        "rows": int(len(raw)),
        "raw_columns": raw_columns,
        "missing_expected": missing_expected,
        "extra_columns": extra_columns,
        "compat_rows": int(len(compat)),
        "compat_columns": list(compat.columns),
        "compat_period_start_min": None if compat.empty else _series_min(compat, "period_start_at"),
        "compat_period_end_min": None if compat.empty else _series_min(compat, "period_end_at"),
        "compat_observed_min": None if compat.empty else _series_min(compat, "observed_at"),
    }


def _series_min(df: pd.DataFrame, column: str) -> str | None:
    if column not in df.columns:
        return None
    series = cast(pd.Series, pd.to_datetime(cast(pd.Series, df[column]), errors="coerce"))
    value = series.min()
    if pd.isna(value):
        return None
    timestamp = cast(pd.Timestamp, pd.Timestamp(cast(Any, value)))
    return timestamp.isoformat(sep=" ")


def _sample_report(parquets: list[Path], sample_count: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for path in parquets[:sample_count]:
        item = _summarize_parquet(path)
        item["station_name"] = _parse_station_name_from_parquet(path)
        samples.append(item)
    return samples


def _run_river_meta_scan(output_root: Path, parquets: list[Path]) -> dict[str, Any]:
    case_root = output_root.parent / f"{output_root.name}_river_meta_case"
    parquet_dir = case_root / "parquet"
    if case_root.exists():
        shutil.rmtree(case_root)
    parquet_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for path in parquets[:12]:
        shutil.copy2(path, parquet_dir / path.name)
        copied += 1

    entries = scan_parquet_dir(case_root)
    sample_load = None
    sample = next((p for p in sorted(parquet_dir.glob("*.parquet")) if p.name.startswith("jma_")), None)
    if sample is not None:
        frame = load_records_parquet(sample)
        sample_load = {
            "path": str(sample),
            "rows": int(len(frame)),
            "columns": list(frame.columns),
            "period_start_min": None if frame.empty else _series_min(frame, "period_start_at"),
            "period_end_min": None if frame.empty else _series_min(frame, "period_end_at"),
            "observed_min": None if frame.empty else _series_min(frame, "observed_at"),
        }

    return {
        "case_root": str(case_root),
        "copied_files": copied,
        "entries": len(entries),
        "first_entries": [
            {
                "source": entry.source,
                "station_key": entry.station_key,
                "station_name": entry.station_name,
                "year": entry.year,
                "months": entry.months,
                "complete": entry.complete,
            }
            for entry in entries[:8]
        ],
        "sample_load": sample_load,
    }


def _run_hydrology_graphs_scan(output_root: Path) -> dict[str, Any]:
    catalog = scan_parquet_catalog(output_root)
    return {
        "rows": int(len(catalog.data)),
        "stations": catalog.stations[:5],
        "base_dates_count": len(catalog.base_dates),
        "invalid_files_count": len(catalog.invalid_files),
        "warnings_count": len(catalog.warnings),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Parquet 出力と読み込みの整合性を確認する")
    parser.add_argument("--output-root", default="data/parquet_input", help="Parquet 出力先ルート")
    parser.add_argument("--temp-root", default="tmp/parquet_input_build", help="生成時の一時ルート")
    parser.add_argument("--report", default="tmp/parquet_input_verify/report.json", help="検証レポートの保存先")
    parser.add_argument("--seed", type=int, default=None, help="生成時の乱数シード")
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="既存の parquet をそのまま検証し、生成処理は実行しない",
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=6,
        help="レポートへ出すサンプル parquet 件数",
    )
    args = parser.parse_args()

    repo = _repo_root()
    output_root = (repo / args.output_root).resolve()
    temp_root = (repo / args.temp_root).resolve()
    report_path = (repo / args.report).resolve()

    generation_result: dict[str, Any] | None = None
    if not args.skip_generate:
        output_root.mkdir(parents=True, exist_ok=True)
        temp_root.mkdir(parents=True, exist_ok=True)
        returncode, generation_result = _generate_parquets(repo, output_root, temp_root, args.seed)
        if returncode != 0:
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(
                json.dumps(generation_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(generation_result["stdout"])
            print(generation_result["stderr"])
            return returncode

    if not output_root.exists():
        raise SystemExit(f"output root not found: {output_root}")

    parquets = sorted(output_root.glob("*.parquet"))
    if not parquets:
        raise SystemExit(f"no parquet files found in: {output_root}")

    report = {
        "output_root": str(output_root),
        "parquet_count": len(parquets),
        "generation": generation_result,
        "samples": _sample_report(parquets, max(1, args.sample_count)),
        "river_meta_scan": _run_river_meta_scan(output_root, parquets),
        "hydrology_graphs_scan": _run_hydrology_graphs_scan(output_root),
    }

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "output_root": str(output_root),
                "parquet_count": len(parquets),
                "sample_count": len(report["samples"]),
                "river_meta_entries": report["river_meta_scan"]["entries"],
                "hydro_rows": report["hydrology_graphs_scan"]["rows"],
            },
            ensure_ascii=False,
        )
    )
    print(report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
