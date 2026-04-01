"""Parquet 中間保存・読み込みモジュール。

観測所×年（または観測所×年×月）ごとに Parquet ファイルを保存し、
途中停止からの再開やExcel 再生成時のデータ再利用を可能にする。
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from ..domain.models import RainfallRecord

# Parquet 旧形式カラム（互換読み込み用）
PARQUET_COLUMNS = [
    "source",
    "station_key",
    "station_name",
    "observed_at",
    "interval",
    "rainfall_mm",
    "quality",
]

UNIFIED_PARQUET_COLUMNS = [
    "source",
    "station_key",
    "station_name",
    "observed_at",
    "metric",
    "value",
    "unit",
    "interval",
    "quality",
]

COMPAT_OUTPUT_COLUMNS = [
    "source",
    "station_key",
    "station_name",
    "observed_at",
    "interval",
    "metric",
    "value",
    "unit",
    "quality",
    "rainfall_mm",
]


def build_parquet_path(
    output_dir: str | Path,
    source: str,
    station_key: str,
    year: int,
    *,
    month: int | None = None,
) -> Path:
    """Parquet ファイルのパスを生成する。

    month を指定すると月単位ファイル、省略すると年単位ファイルになる。
    """
    safe_key = str(station_key).replace("/", "_").replace("\\", "_")
    if month is not None:
        return Path(output_dir) / "parquet" / f"{source}_{safe_key}_{year}_{month:02d}.parquet"
    return Path(output_dir) / "parquet" / f"{source}_{safe_key}_{year}.parquet"


def save_records_parquet(records: list[RainfallRecord], output_path: str | Path) -> Path:
    """RainfallRecord のリストを共通スキーマ (`metric/value`) で保存する。"""
    unified_rows = []
    for r in records:
        unified_rows.append({
            "source": r.source,
            "station_key": r.station_key,
            "station_name": r.station_name,
            "observed_at": r.observed_at,
            "metric": "rainfall",
            "value": r.rainfall_mm,
            "unit": "mm",
            "interval": r.interval,
            "quality": r.quality,
        })
    return save_unified_records_parquet(unified_rows, output_path)


def save_unified_records_parquet(records: list[dict], output_path: str | Path) -> Path:
    """共通時系列レコードを Parquet ファイルに保存する。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records, columns=UNIFIED_PARQUET_COLUMNS)
    if df.empty:
        df = pd.DataFrame(columns=UNIFIED_PARQUET_COLUMNS)
    for column in UNIFIED_PARQUET_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA
    df["metric"] = df["metric"].fillna("").astype(str).str.strip().replace("", "rainfall")
    df["unit"] = df["unit"].fillna("").astype(str).str.strip().replace("", "mm")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    if "quality" not in df.columns:
        df["quality"] = pd.NA
    missing_quality = df["quality"].isna() | (df["quality"].fillna("").astype(str).str.strip() == "")
    df.loc[missing_quality & df["value"].isna(), "quality"] = "missing"
    df.loc[missing_quality & df["value"].notna(), "quality"] = "normal"
    df = df[UNIFIED_PARQUET_COLUMNS]
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


def load_records_parquet(parquet_path: str | Path) -> pd.DataFrame:
    """Parquet ファイルから DataFrame を読み込む。"""
    path = Path(parquet_path)
    if not path.exists():
        return pd.DataFrame(columns=COMPAT_OUTPUT_COLUMNS)
    df = pd.read_parquet(path, engine="pyarrow")
    return _to_compat_dataframe(df)


def parquet_exists(
    output_dir: str | Path,
    source: str,
    station_key: str,
    year: int,
    *,
    month: int | None = None,
) -> bool:
    """指定された Parquet ファイルが存在するか確認する。"""
    path = build_parquet_path(output_dir, source, station_key, year, month=month)
    return path.exists() and path.stat().st_size > 0


def load_and_concat_monthly_parquets(
    output_dir: str | Path,
    source: str,
    station_key: str,
    year: int,
) -> pd.DataFrame:
    """1〜12月の月別 Parquet を結合して1年分の DataFrame を返す。

    存在する月のみ結合する。全月が存在しない場合は空 DataFrame を返す。
    """
    dfs = []
    for m in range(1, 13):
        pq_path = build_parquet_path(output_dir, source, station_key, year, month=m)
        if pq_path.exists() and pq_path.stat().st_size > 0:
            df = load_records_parquet(pq_path)
            if not df.empty:
                dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=COMPAT_OUTPUT_COLUMNS)
    combined = pd.concat(dfs, ignore_index=True)
    combined.sort_values("observed_at", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


def _to_compat_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    for column in ("source", "station_key", "station_name", "interval", "quality"):
        if column not in work.columns:
            work[column] = pd.NA
    if "observed_at" in work.columns:
        work["observed_at"] = pd.to_datetime(work["observed_at"], errors="coerce")
    else:
        work["observed_at"] = pd.NaT

    has_unified = {"metric", "value"}.issubset(set(work.columns))
    has_legacy = "rainfall_mm" in work.columns

    if has_unified:
        work["metric"] = work["metric"].fillna("").astype(str).str.strip().replace("", "rainfall")
        work["value"] = pd.to_numeric(work["value"], errors="coerce")
        if "unit" not in work.columns:
            work["unit"] = pd.NA
        work["unit"] = work["unit"].fillna("").astype(str).str.strip().replace("", "mm")
    elif has_legacy:
        work["metric"] = "rainfall"
        work["value"] = pd.to_numeric(work["rainfall_mm"], errors="coerce")
        work["unit"] = "mm"
    else:
        work["metric"] = "rainfall"
        work["value"] = pd.NA
        work["unit"] = "mm"

    if has_legacy:
        work["rainfall_mm"] = pd.to_numeric(work["rainfall_mm"], errors="coerce")
    else:
        metric_series = work["metric"].astype(str)
        value_series = pd.to_numeric(work["value"], errors="coerce")
        work["rainfall_mm"] = value_series.where(metric_series.eq("rainfall"))

    missing_quality = work["quality"].isna() | (work["quality"].fillna("").astype(str).str.strip() == "")
    work.loc[missing_quality & work["value"].isna(), "quality"] = "missing"
    work.loc[missing_quality & work["value"].notna(), "quality"] = "normal"

    for column in COMPAT_OUTPUT_COLUMNS:
        if column not in work.columns:
            work[column] = pd.NA
    return work[COMPAT_OUTPUT_COLUMNS]


def migrate_legacy_jma_parquets(
    output_dir: str | Path,
    old_block_number: str,
    new_station_key: str,
    year: int,
) -> int:
    """旧フォーマット (block_number のみ) の Parquet を新フォーマットにリネームする。

    旧: jma_{block_number}_{year}.parquet / jma_{block_number}_{year}_{month}.parquet
    新: jma_{new_station_key}_{year}.parquet / jma_{new_station_key}_{year}_{month}.parquet

    Returns: リネームしたファイル数
    """
    parquet_dir = Path(output_dir) / "parquet"
    if not parquet_dir.exists():
        return 0

    renamed = 0
    safe_old = str(old_block_number).replace("/", "_").replace("\\", "_")
    safe_new = str(new_station_key).replace("/", "_").replace("\\", "_")

    # 年単位ファイル: jma_{old}_{year}.parquet
    old_yearly = parquet_dir / f"jma_{safe_old}_{year}.parquet"
    new_yearly = parquet_dir / f"jma_{safe_new}_{year}.parquet"
    if old_yearly.exists() and not new_yearly.exists():
        old_yearly.rename(new_yearly)
        renamed += 1

    # 月単位ファイル: jma_{old}_{year}_{mm}.parquet
    for month in range(1, 13):
        old_monthly = parquet_dir / f"jma_{safe_old}_{year}_{month:02d}.parquet"
        new_monthly = parquet_dir / f"jma_{safe_new}_{year}_{month:02d}.parquet"
        if old_monthly.exists() and not new_monthly.exists():
            old_monthly.rename(new_monthly)
            renamed += 1

    return renamed


# ---------------------------------------------------------------------------
# Parquet スキャン
# ---------------------------------------------------------------------------

# ファイル名パターン:
#   月単位: {source}_{station_key}_{year}_{month:02d}.parquet
#   年単位: {source}_{station_key}_{year}.parquet
_MONTHLY_PATTERN = re.compile(
    r"^(?P<source>[a-z_]+)_(?P<station_key>.+)_(?P<year>\d{4})_(?P<month>\d{2})\.parquet$"
)
_YEARLY_PATTERN = re.compile(
    r"^(?P<source>[a-z_]+)_(?P<station_key>.+)_(?P<year>\d{4})\.parquet$"
)
_RANGE_WITH_METRIC_PATTERN = re.compile(
    r"^(?P<source>[a-z_]+)_(?P<station_key>.+?)_"
    r"(?P<metric>rainfall|water_level|discharge)_(?P<interval>10min|1hour|1day)_"
    r"(?P<start_ym>\d{6})_(?P<end_ym>\d{6})\.parquet$"
)
_RANGE_NO_METRIC_PATTERN = re.compile(
    r"^(?P<source>[a-z_]+)_(?P<station_key>.+?)_"
    r"(?P<interval>10min|1hour|1day)_"
    r"(?P<start_ym>\d{6})_(?P<end_ym>\d{6})\.parquet$"
)


@dataclass(frozen=True)
class ParquetEntry:
    """Parquet スキャン結果の1エントリ（観測所×年）。"""
    source: str
    station_key: str
    year: int
    station_name: str = ""
    months: list[int] = field(default_factory=list)
    complete: bool = False


def scan_parquet_dir(output_dir: str | Path) -> list[ParquetEntry]:
    """parquet/ 配下をスキャンして観測所×年ごとの状態を返す。"""
    base_path = Path(output_dir)
    parquet_dir = base_path if base_path.name.lower() == "parquet" else base_path / "parquet"
    if not parquet_dir.exists():
        return []

    # (source, station_key, year) -> set of months
    index: dict[tuple[str, str, int], set[int]] = {}
    station_names: dict[tuple[str, str], str] = {}
    with os.scandir(parquet_dir) as entries:
        for entry in entries:
            if not entry.is_file() or not entry.name.endswith(".parquet"):
                continue
            try:
                if entry.stat().st_size == 0:
                    continue
            except OSError:
                continue

            parsed = _parse_parquet_filename(entry.name)
            if parsed is None:
                continue
            first = parsed[0]
            pair = (first[0], first[1])
            if pair not in station_names:
                station_names[pair] = _read_station_name_from_parquet(Path(entry.path))
            for source, station_key, year, month in parsed:
                key = (source, station_key, year)
                if month is None:
                    index.setdefault(key, set())
                else:
                    index.setdefault(key, set()).add(month)

    entries: list[ParquetEntry] = []
    for (source, station_key, year), months in sorted(index.items()):
        sorted_months = sorted(months)
        # 旧年単位 water_info（months が空）は complete 扱い。
        # それ以外は、月情報が 12 件揃うと complete。
        complete = (source == "water_info" and len(sorted_months) == 0) or (len(sorted_months) == 12)
        entries.append(ParquetEntry(
            source=source, station_key=station_key, year=year,
            station_name=station_names.get((source, station_key), ""),
            months=sorted_months, complete=complete,
        ))

    return entries


def find_missing_months(
    output_dir: str | Path,
    source: str,
    station_key: str,
    year: int,
) -> list[int]:
    """JMA の場合に1〜12のうち欠けている月番号リストを返す。water_info は常に空リスト。"""
    if source == "water_info":
        pq_path = build_parquet_path(output_dir, source, station_key, year)
        return [] if (pq_path.exists() and pq_path.stat().st_size > 0) else [0]

    present = set()
    for m in range(1, 13):
        if parquet_exists(output_dir, source, station_key, year, month=m):
            present.add(m)
    return [m for m in range(1, 13) if m not in present]


def _parse_parquet_filename(name: str) -> list[tuple[str, str, int, int | None]] | None:
    """既存/新規のファイル命名から (source, station_key, year, month?) を抽出する。"""

    m = _MONTHLY_PATTERN.match(name)
    if m:
        return [(
            m.group("source"),
            m.group("station_key"),
            int(m.group("year")),
            int(m.group("month")),
        )]

    m = _YEARLY_PATTERN.match(name)
    if m:
        return [(
            m.group("source"),
            m.group("station_key"),
            int(m.group("year")),
            None,
        )]

    m = _RANGE_WITH_METRIC_PATTERN.match(name)
    if m:
        return _expand_ym_range(
            source=m.group("source"),
            station_key=m.group("station_key"),
            start_ym=m.group("start_ym"),
            end_ym=m.group("end_ym"),
        )

    m = _RANGE_NO_METRIC_PATTERN.match(name)
    if m:
        return _expand_ym_range(
            source=m.group("source"),
            station_key=m.group("station_key"),
            start_ym=m.group("start_ym"),
            end_ym=m.group("end_ym"),
        )
    return None


def _expand_ym_range(
    *,
    source: str,
    station_key: str,
    start_ym: str,
    end_ym: str,
) -> list[tuple[str, str, int, int | None]]:
    """YYYYMM の範囲を年×月へ展開する。"""

    try:
        start_year = int(start_ym[:4])
        start_month = int(start_ym[4:6])
        end_year = int(end_ym[:4])
        end_month = int(end_ym[4:6])
    except Exception:  # noqa: BLE001
        return []
    if not (1 <= start_month <= 12 and 1 <= end_month <= 12):
        return []
    if (end_year, end_month) < (start_year, start_month):
        return []

    result: list[tuple[str, str, int, int | None]] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        result.append((source, station_key, year, month))
        month += 1
        if month == 13:
            month = 1
            year += 1
    return result


def _read_station_name_from_parquet(path: Path) -> str:
    """Parquet 先頭行の station_name を返す。読めない場合は空文字。"""

    try:
        df = pd.read_parquet(path, engine="pyarrow", columns=["station_name"])
    except Exception:  # noqa: BLE001
        return ""
    if df is None or df.empty or "station_name" not in df.columns:
        return ""
    token = str(df["station_name"].iloc[0] or "").strip()
    return token
