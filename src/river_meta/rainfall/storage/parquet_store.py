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

# Parquet に保存するカラム（raw は除外してファイルサイズを抑える）
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
    """RainfallRecord のリストを Parquet ファイルに保存する。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    for r in records:
        rows.append({
            "source": r.source,
            "station_key": r.station_key,
            "station_name": r.station_name,
            "observed_at": r.observed_at,
            "interval": r.interval,
            "rainfall_mm": r.rainfall_mm,
            "quality": r.quality,
        })

    df = pd.DataFrame(rows, columns=PARQUET_COLUMNS)
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


def save_unified_records_parquet(records: list[dict], output_path: str | Path) -> Path:
    """共通時系列レコードを Parquet ファイルに保存する。"""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records, columns=UNIFIED_PARQUET_COLUMNS)
    if df.empty:
        df = pd.DataFrame(columns=UNIFIED_PARQUET_COLUMNS)
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    df.to_parquet(path, engine="pyarrow", index=False)
    return path


def load_records_parquet(parquet_path: str | Path) -> pd.DataFrame:
    """Parquet ファイルから DataFrame を読み込む。"""
    path = Path(parquet_path)
    if not path.exists():
        return pd.DataFrame(columns=PARQUET_COLUMNS)
    df = pd.read_parquet(path, engine="pyarrow")
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    return df


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
        return pd.DataFrame(columns=PARQUET_COLUMNS)
    combined = pd.concat(dfs, ignore_index=True)
    combined.sort_values("observed_at", inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


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


@dataclass(frozen=True)
class ParquetEntry:
    """Parquet スキャン結果の1エントリ（観測所×年）。"""
    source: str
    station_key: str
    year: int
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
    with os.scandir(parquet_dir) as entries:
        for entry in entries:
            if not entry.is_file() or not entry.name.endswith(".parquet"):
                continue
            try:
                if entry.stat().st_size == 0:
                    continue
            except OSError:
                continue

            m = _MONTHLY_PATTERN.match(entry.name)
            if m:
                key = (m.group("source"), m.group("station_key"), int(m.group("year")))
                index.setdefault(key, set()).add(int(m.group("month")))
                continue

            m = _YEARLY_PATTERN.match(entry.name)
            if m:
                key = (m.group("source"), m.group("station_key"), int(m.group("year")))
                index.setdefault(key, set())
                continue

    entries: list[ParquetEntry] = []
    for (source, station_key, year), months in sorted(index.items()):
        sorted_months = sorted(months)
        if source == "water_info":
            # water_info は年単位ファイルなので常に complete
            entries.append(ParquetEntry(
                source=source, station_key=station_key, year=year,
                months=[], complete=True,
            ))
        else:
            # JMA: 12ヶ月揃っていれば complete
            entries.append(ParquetEntry(
                source=source, station_key=station_key, year=year,
                months=sorted_months, complete=(len(sorted_months) == 12),
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
