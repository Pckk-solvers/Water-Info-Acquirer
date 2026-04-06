from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from .parquet_exporter import build_jma_unified_records


def _safe_token(value: str) -> str:
    return str(value).replace("/", "_").replace("\\", "_")


def _to_interval_token(interval_label: str) -> str:
    if interval_label == "daily":
        return "1day"
    if interval_label == "hourly":
        return "1hour"
    return "10min"


def export_weather_ndjson(
    records_source: Any,
    *,
    prec_no: str,
    block_no: str,
    interval_label: str,
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> Path:
    station_key = f"{prec_no}_{block_no}"
    interval = _to_interval_token(interval_label)
    file_name = (
        f"jma_{_safe_token(station_key)}_{interval}_"
        f"{start_date.strftime('%Y%m')}_{end_date.strftime('%Y%m')}.ndjson"
    )
    output_path = output_dir / file_name
    output_path.parent.mkdir(parents=True, exist_ok=True)

    records = build_jma_unified_records(
        records_source,
        prec_no=str(prec_no),
        block_no=str(block_no),
        interval_label=interval_label,
    )
    df = pd.DataFrame(records)
    if not df.empty:
        for col in ("period_start_at", "period_end_at", "observed_at"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
    df.to_json(
        output_path,
        orient="records",
        lines=True,
        force_ascii=False,
        date_format="iso",
    )
    return output_path
