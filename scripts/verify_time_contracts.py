"""Run reusable end-to-end verification for time-contract outputs.

Usage:
    uv run python scripts/verify_time_contracts.py
"""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class VerificationCase:
    name: str
    command: list[str]
    output_dir: Path
    csv_glob: str
    parquet_glob: str
    excel_glob: str


def _build_cases(output_root: Path) -> list[VerificationCase]:
    return [
        VerificationCase(
            name="water_info_s_hourly",
            command=[
                sys.executable,
                "-m",
                "water_info",
                "fetch",
                "--code",
                "303051283310090",
                "--mode",
                "S",
                "--start",
                "2025-01",
                "--end",
                "2025-01",
                "--interval",
                "hourly",
                "--csv",
                "--excel",
                "--parquet",
                "--output-dir",
                str(output_root / "water_s"),
            ],
            output_dir=output_root / "water_s",
            csv_glob="*.csv",
            parquet_glob="parquet/*.parquet",
            excel_glob="*.xlsx",
        ),
        VerificationCase(
            name="water_info_u_hourly",
            command=[
                sys.executable,
                "-m",
                "water_info",
                "fetch",
                "--code",
                "101031281101620",
                "--mode",
                "U",
                "--start",
                "2024-06",
                "--end",
                "2024-06",
                "--interval",
                "hourly",
                "--csv",
                "--excel",
                "--parquet",
                "--output-dir",
                str(output_root / "water_u"),
            ],
            output_dir=output_root / "water_u",
            csv_glob="*.csv",
            parquet_glob="parquet/*.parquet",
            excel_glob="*.xlsx",
        ),
        VerificationCase(
            name="jma_hourly",
            command=[
                sys.executable,
                "-m",
                "jma_rainfall_pipeline",
                "fetch",
                "--station",
                "13:47406:s1",
                "--start",
                "2026-03-01",
                "--end",
                "2026-03-03",
                "--interval",
                "hourly",
                "--csv",
                "--excel",
                "--parquet",
                "--output-dir",
                str(output_root / "jma_hourly"),
            ],
            output_dir=output_root / "jma_hourly",
            csv_glob="csv/*.csv",
            parquet_glob="parquet/*.parquet",
            excel_glob="excel/*.xlsx",
        ),
        VerificationCase(
            name="jma_10min",
            command=[
                sys.executable,
                "-m",
                "jma_rainfall_pipeline",
                "fetch",
                "--station",
                "13:47406:s1",
                "--start",
                "2026-03-01",
                "--end",
                "2026-03-03",
                "--interval",
                "10min",
                "--csv",
                "--excel",
                "--parquet",
                "--output-dir",
                str(output_root / "jma_10min"),
            ],
            output_dir=output_root / "jma_10min",
            csv_glob="csv/*.csv",
            parquet_glob="parquet/*.parquet",
            excel_glob="excel/*.xlsx",
        ),
    ]


def _pick_one(base_dir: Path, pattern: str) -> Path:
    matches = sorted(base_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"pattern not found: {base_dir / pattern}")
    return matches[0]


def _read_preview(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    xls = pd.ExcelFile(path)
    target_sheet = next((name for name in xls.sheet_names if name not in {"summary", "出典"}), xls.sheet_names[0])
    return pd.read_excel(path, sheet_name=target_sheet)


def _series_min(df: pd.DataFrame, column: str) -> str | None:
    if column not in df.columns:
        return None
    value = pd.to_datetime(df[column], errors="coerce").min()
    if pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat(sep=" ")


def _hour_max(df: pd.DataFrame) -> int | None:
    if "hour" not in df.columns:
        return None
    values = pd.to_numeric(df["hour"], errors="coerce").dropna()
    if values.empty:
        return None
    return int(values.max())


def _summarize_frame(df: pd.DataFrame) -> dict[str, Any]:
    preview = df.head(3).copy()
    return {
        "rows": int(len(df)),
        "columns": list(df.columns),
        "period_start_min": _series_min(df, "period_start_at"),
        "period_end_min": _series_min(df, "period_end_at"),
        "observed_min": _series_min(df, "observed_at"),
        "datetime_min": _series_min(df, "datetime"),
        "hour_max": _hour_max(df),
        "head": _json_safe(preview.to_dict(orient="records")),
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if value is None:
        return None
    if value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat(sep=" ")
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _run_case(case: VerificationCase) -> dict[str, Any]:
    case.output_dir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(case.command, capture_output=True, text=True, encoding="cp932", errors="replace")
    result: dict[str, Any] = {
        "name": case.name,
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "command": case.command,
    }
    if proc.returncode != 0:
        return result

    csv_path = _pick_one(case.output_dir, case.csv_glob)
    parquet_path = _pick_one(case.output_dir, case.parquet_glob)
    excel_path = _pick_one(case.output_dir, case.excel_glob)
    result["artifacts"] = {
        "csv": str(csv_path),
        "parquet": str(parquet_path),
        "excel": str(excel_path),
    }
    result["csv_summary"] = _summarize_frame(_read_preview(csv_path))
    result["parquet_summary"] = _summarize_frame(_read_preview(parquet_path))
    result["excel_summary"] = _summarize_frame(_read_preview(excel_path))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="時間契約の実データ検証を再実行する")
    parser.add_argument(
        "--output-root",
        default="tmp/verify_time",
        help="検証出力の保存先ルート",
    )
    parser.add_argument(
        "--report",
        default="tmp/verify_time/report.json",
        help="検証サマリ JSON の保存先",
    )
    args = parser.parse_args()

    output_root = Path(args.output_root)
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    results = [_run_case(case) for case in _build_cases(output_root)]
    payload = {"output_root": str(output_root), "results": results}
    report_path.write_text(json.dumps(_json_safe(payload), ensure_ascii=False, indent=2), encoding="utf-8")

    for item in results:
        print(f"[{item['name']}] returncode={item['returncode']}")
        if item["returncode"] != 0:
            stderr = str(item.get("stderr", "")).strip()
            print(stderr or "failed")
            continue
        csv_summary = item["csv_summary"]
        parquet_summary = item["parquet_summary"]
        print(
            f"csv_rows={csv_summary['rows']} "
            f"csv_observed_min={csv_summary['observed_min']} "
            f"parquet_period_start_min={parquet_summary['period_start_min']} "
            f"parquet_period_end_min={parquet_summary['period_end_min']} "
            f"hour_max={csv_summary['hour_max']}"
        )
    print(f"report={report_path}")
    return 0 if all(item["returncode"] == 0 for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
