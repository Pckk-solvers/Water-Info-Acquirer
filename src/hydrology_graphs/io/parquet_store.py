from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
import os
from pathlib import Path
import threading
from typing import cast

import pandas as pd

"""Parquet から時系列カタログを構築する処理。

旧スキーマの救済もここで行い、最終的には正規化済みの DataFrame として返す。
"""

_REQUIRED_COLUMNS = (
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
)

_ALLOWED_SOURCES = {"jma", "water_info"}
_ALLOWED_METRICS = {"rainfall", "water_level", "discharge"}
_ALLOWED_INTERVALS = {"10min", "1hour", "1day"}
_ALLOWED_QUALITIES = {"normal", "missing"}
_METRIC_LABELS = {
    "rainfall": "雨量",
    "discharge": "流量",
    "water_level": "水位",
}
_METRIC_LABEL_ORDER = ("雨量", "流量", "水位")

_LEGACY_VALUE_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("rainfall_mm", "rainfall", "mm"),
    ("precipitation_mm", "rainfall", "mm"),
    ("precipitation", "rainfall", "mm"),
    ("rainfall", "rainfall", "mm"),
    ("water_level_m", "water_level", "m"),
    ("water_level", "water_level", "m"),
    ("discharge_m3s", "discharge", "m3/s"),
    ("discharge", "discharge", "m3/s"),
)


def _to_datetime_series(values: pd.Series) -> pd.Series:
    """pandas 2系の混在フォーマット差異を吸収して日時化する。"""

    try:
        return pd.to_datetime(values, errors="coerce", format="mixed")
    except TypeError:
        return pd.to_datetime(values, errors="coerce")


def _interval_hours(interval: str) -> float:
    if interval == "1day":
        return 24.0
    if interval == "1hour":
        return 1.0
    return 10.0 / 60.0


@dataclass(slots=True)
class _CatalogCacheEntry:
    """ディレクトリスキャン結果のキャッシュ。"""

    fingerprint: tuple[tuple[str, int, int], ...]
    catalog: "ParquetCatalog"


@dataclass(slots=True)
class _FileScanResult:
    """1ファイル分のスキャン結果。"""

    path: str
    frame: pd.DataFrame
    errors: list[str]
    warning: str | None = None
    metric_candidates: tuple[str, ...] = ()


_CATALOG_CACHE: dict[str, _CatalogCacheEntry] = {}
_STATION_INDEX_CACHE: dict[str, _CatalogCacheEntry] = {}
_CATALOG_CACHE_LOCK = threading.Lock()


@dataclass(slots=True)
class ParquetCatalog:
    """スキャン済み Parquet 群を束ねたカタログ。"""

    data: pd.DataFrame
    invalid_files: dict[str, list[str]]
    warnings: list[str]
    station_metric_labels: dict[tuple[str, str], tuple[str, ...]] = field(default_factory=dict)

    @property
    def stations(self) -> list[tuple[str, str, str]]:
        """存在する観測所一覧を source / station_key / station_name で返す。"""

        if self.data.empty:
            return []
        required = {"source", "station_key", "station_name"}
        if not required.issubset(set(self.data.columns)):
            return []
        rows = self.data[["source", "station_key", "station_name"]].drop_duplicates().itertuples(index=False, name=None)
        sorted_rows = sorted(rows, key=lambda r: (str(r[0]), str(r[1])))
        return [(str(src), str(key), str(name or "")) for src, key, name in sorted_rows]

    @property
    def base_dates(self) -> list[str]:
        """データ内に含まれる日付一覧を ISO 文字列で返す。"""

        if self.data.empty:
            return []
        if "period_end_at" in self.data.columns:
            period_end = _to_datetime_series(cast(pd.Series, self.data["period_end_at"]))
            observed = _to_datetime_series(cast(pd.Series, self.data["observed_at"]))
            time_values = period_end.fillna(observed)
        elif "observed_at" in self.data.columns:
            time_values = _to_datetime_series(cast(pd.Series, self.data["observed_at"]))
        else:
            return []
        observed = time_values.dropna()
        if observed.empty:
            return []
        return sorted({ts.date().isoformat() for ts in observed})

    def select(
        self,
        *,
        source: str,
        station_key: str,
        metric: str,
        interval: str | None = None,
    ) -> pd.DataFrame:
        """条件に合う時系列だけを抽出する。"""

        mask = (
            (self.data["source"] == source)
            & (self.data["station_key"] == station_key)
            & (self.data["metric"] == metric)
        )
        if interval is not None:
            mask = mask & (self.data["interval"] == interval)
        selected = self.data.loc[mask].copy()
        if selected.empty:
            return selected
        if "period_end_at" in selected.columns:
            selected["period_end_at"] = _to_datetime_series(cast(pd.Series, selected["period_end_at"]))
        selected["observed_at"] = _to_datetime_series(cast(pd.Series, selected["observed_at"]))
        selected["value"] = pd.to_numeric(selected["value"], errors="coerce")
        selected = selected.dropna(subset=["observed_at"])
        if "period_end_at" in selected.columns:
            selected["_sort_at"] = selected["period_end_at"].fillna(selected["observed_at"])
            sorted_df = selected.sort_values(by="_sort_at").drop(columns="_sort_at")
            return sorted_df.reset_index(drop=True)
        return selected.sort_values(by="observed_at").reset_index(drop=True)


def scan_parquet_catalog(parquet_dir: str | Path) -> ParquetCatalog:
    """ディレクトリ配下の Parquet を走査してカタログ化する。"""

    root, file_infos, stat_errors, fingerprint, cache_key = _prepare_scan(parquet_dir)
    if not stat_errors:
        with _CATALOG_CACHE_LOCK:
            cached = _CATALOG_CACHE.get(cache_key)
        if cached and cached.fingerprint == fingerprint:
            return cached.catalog

    scan_results = _scan_files_parallel([path for path, _, _ in file_infos], columns=None)
    frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    invalid_files: dict[str, list[str]] = dict(stat_errors)
    for result in scan_results:
        if result.errors:
            invalid_files[result.path] = result.errors
        if not result.frame.empty:
            frames.append(result.frame)
        elif result.warning:
            warnings.append(result.warning)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=pd.Index(_REQUIRED_COLUMNS))
    catalog = ParquetCatalog(
        data=merged,
        invalid_files=invalid_files,
        warnings=warnings,
        station_metric_labels=_build_station_metric_labels(merged),
    )
    if not stat_errors:
        with _CATALOG_CACHE_LOCK:
            _CATALOG_CACHE[cache_key] = _CatalogCacheEntry(fingerprint=fingerprint, catalog=catalog)
    return catalog


def scan_parquet_station_index(parquet_dir: str | Path) -> ParquetCatalog:
    """観測所一覧表示向けに最小列だけを軽量走査する。"""

    root, file_infos, stat_errors, fingerprint, cache_key = _prepare_scan(parquet_dir)
    if not stat_errors:
        with _CATALOG_CACHE_LOCK:
            cached = _STATION_INDEX_CACHE.get(cache_key)
        if cached and cached.fingerprint == fingerprint:
            return cached.catalog

    station_columns = ["source", "station_key", "station_name"]
    scan_results = _scan_files_parallel([path for path, _, _ in file_infos], columns=station_columns)
    frames: list[pd.DataFrame] = []
    warnings: list[str] = []
    invalid_files: dict[str, list[str]] = dict(stat_errors)
    for result in scan_results:
        if result.errors:
            invalid_files[result.path] = result.errors
        if not result.frame.empty:
            frames.append(result.frame)
        elif result.warning:
            warnings.append(result.warning)

    merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=pd.Index(station_columns))
    # 軽量走査は一覧表示が目的のため、最小限の型整形のみ行う。
    if not merged.empty:
        merged["source"] = merged["source"].astype(str)
        merged["station_key"] = merged["station_key"].astype(str)
        merged["station_name"] = merged["station_name"].fillna("").astype(str)
        if "metric_candidates" in merged.columns:
            merged["metric_candidates"] = merged["metric_candidates"].apply(_normalize_metric_candidates_cell)
        else:
            merged["metric_candidates"] = pd.Series([()] * len(merged), index=merged.index, dtype=object)
        merged = merged.loc[merged["source"].isin(list(_ALLOWED_SOURCES))].copy()
    catalog = ParquetCatalog(
        data=merged,
        invalid_files=invalid_files,
        warnings=warnings,
        station_metric_labels=_build_station_metric_labels(merged),
    )
    if not stat_errors:
        with _CATALOG_CACHE_LOCK:
            _STATION_INDEX_CACHE[cache_key] = _CatalogCacheEntry(fingerprint=fingerprint, catalog=catalog)
    return catalog


def _prepare_scan(
    parquet_dir: str | Path,
) -> tuple[Path, list[tuple[Path, int, int]], dict[str, list[str]], tuple[tuple[str, int, int], ...], str]:
    """走査対象のファイル情報とフィンガープリントを組み立てる。"""

    root = Path(parquet_dir)
    if not root.exists():
        raise FileNotFoundError(f"Parquet directory not found: {root}")

    files = sorted(root.rglob("*.parquet"))
    file_infos: list[tuple[Path, int, int]] = []
    stat_errors: dict[str, list[str]] = {}
    for path in files:
        try:
            stat = path.stat()
        except OSError as exc:
            stat_errors[str(path)] = [f"stat_error: {type(exc).__name__}: {exc}"]
            continue
        file_infos.append((path, int(stat.st_size), int(stat.st_mtime_ns)))

    fingerprint = tuple((str(path), size, mtime_ns) for path, size, mtime_ns in file_infos)
    cache_key = str(root.resolve())
    return root, file_infos, stat_errors, fingerprint, cache_key


def _scan_files_parallel(paths: list[Path], *, columns: list[str] | None) -> list[_FileScanResult]:
    """複数Parquetを並列に走査する。"""

    if not paths:
        return []
    if len(paths) == 1:
        return [_scan_single_file(paths[0], columns=columns)]

    max_workers = min(8, max(2, os.cpu_count() or 4))
    results: list[_FileScanResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_scan_single_file, path, columns=columns) for path in paths]
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda item: item.path)
    return results


def _scan_single_file(path: Path, *, columns: list[str] | None) -> _FileScanResult:
    """1ファイルを読み込み、スキーマ検証後の結果を返す。"""

    path_str = str(path)
    metric_candidates = _infer_metric_candidates_from_filename(path)
    try:
        # ファイル単位で読み込み、壊れた 1 件が全体を止めないようにする。
        raw = cast(pd.DataFrame, pd.read_parquet(path, engine="pyarrow", columns=columns))
    except Exception as exc:  # noqa: BLE001
        return _FileScanResult(
            path=path_str,
            frame=pd.DataFrame(columns=pd.Index(_REQUIRED_COLUMNS)),
            errors=[f"read_error: {type(exc).__name__}: {exc}"],
            metric_candidates=metric_candidates,
        )

    if columns is not None:
        if not raw.empty:
            raw = raw.copy()
            raw["metric_candidates"] = [metric_candidates] * len(raw)
        validated, errors = _validate_station_columns(raw)
    else:
        validated, errors = _validate_and_normalize(raw)
    if validated.empty and not errors:
        return _FileScanResult(
            path=path_str,
            frame=validated,
            errors=[],
            warning=f"empty_parquet: {path}",
            metric_candidates=metric_candidates,
        )
    return _FileScanResult(path=path_str, frame=validated, errors=errors, metric_candidates=metric_candidates)


def _validate_station_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """軽量走査用: 観測所一覧に必要な列だけを検証して返す。"""

    required = ("source", "station_key", "station_name")
    optional = ("metric_candidates",)
    missing = [column for column in required if column not in df.columns]
    if missing:
        return pd.DataFrame(columns=pd.Index(required)), [f"missing_columns: {', '.join(missing)}"]
    keep_columns = [column for column in (*required, *optional) if column in df.columns]
    work = cast(pd.DataFrame, df[keep_columns].copy())
    work["source"] = work["source"].astype(str)
    work["station_key"] = work["station_key"].astype(str)
    work["station_name"] = work["station_name"].fillna("").astype(str)
    bad_source = ~work["source"].isin(list(_ALLOWED_SOURCES))
    if bool(bad_source.any()):
        work = work.loc[~bad_source].copy()
        return work, ["invalid_source"]
    if "metric_candidates" in work.columns:
        work["metric_candidates"] = work["metric_candidates"].apply(_normalize_metric_candidates_cell)
    return work, []


def _validate_and_normalize(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """列不足や値不正を確認し、正規化済み DataFrame を返す。"""

    errors: list[str] = []
    # 旧形式の列名を先に吸収してから、共通スキーマに揃える。
    work = _expand_legacy_schema(df)
    missing = [column for column in _REQUIRED_COLUMNS if column not in work.columns]
    if missing:
        return pd.DataFrame(columns=pd.Index(_REQUIRED_COLUMNS)), [f"missing_columns: {', '.join(missing)}"]

    work = cast(pd.DataFrame, work[list(_REQUIRED_COLUMNS)].copy())
    work["source"] = work["source"].astype(str)
    work["station_key"] = work["station_key"].astype(str)
    work["station_name"] = work["station_name"].fillna("").astype(str)
    work["metric"] = work["metric"].astype(str)
    work["unit"] = work["unit"].fillna("").astype(str)
    work["interval"] = work["interval"].astype(str)
    work["quality"] = work["quality"].astype(str)
    work["period_start_at"] = _to_datetime_series(cast(pd.Series, work["period_start_at"]))
    work["period_end_at"] = _to_datetime_series(cast(pd.Series, work["period_end_at"]))
    work["observed_at"] = _to_datetime_series(cast(pd.Series, work["observed_at"]))
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = _normalize_legacy_jma_hourly_observed_at(work)
    work = _fill_period_columns_from_legacy(work)
    missing_observed = work["observed_at"].isna() & work["period_end_at"].notna()
    work.loc[missing_observed, "observed_at"] = work.loc[missing_observed, "period_end_at"]

    bad_source = ~work["source"].isin(list(_ALLOWED_SOURCES))
    bad_metric = ~work["metric"].isin(list(_ALLOWED_METRICS))
    bad_interval = ~work["interval"].isin(list(_ALLOWED_INTERVALS))
    bad_quality = ~work["quality"].isin(list(_ALLOWED_QUALITIES))
    bad_observed = work["observed_at"].isna()
    requirement_units = {
        "rainfall": "mm",
        "water_level": "m",
        "discharge": "m3/s",
    }
    bad_unit = [
        unit != requirement_units.get(metric, "")
        for metric, unit in zip(work["metric"], work["unit"], strict=True)
    ]
    # 契約に合わない列値はまとめて落とし、後続処理を単純化する。
    if bool(bad_source.any()):
        errors.append("invalid_source")
    if bool(bad_metric.any()):
        errors.append("invalid_metric")
    if bool(bad_interval.any()):
        errors.append("invalid_interval")
    if bool(bad_quality.any()):
        errors.append("invalid_quality")
    if bool(bad_observed.any()):
        errors.append("invalid_observed_at")
    if any(bad_unit):
        errors.append("invalid_unit")
    drop_mask = (
        bad_source
        | bad_metric
        | bad_interval
        | bad_quality
        | bad_observed
        | pd.Series(bad_unit, index=work.index)
    )
    cleaned = cast(pd.DataFrame, work.loc[~drop_mask].copy())
    return cleaned, errors


def _normalize_legacy_jma_hourly_observed_at(df: pd.DataFrame) -> pd.DataFrame:
    """旧JMA hourly時刻の 23:59:59.999999 を翌日 00:00 へ揃える。"""

    if df.empty:
        return df

    work = df.copy()
    base_mask = (work["source"] == "jma") & (work["interval"] == "1hour")
    if not bool(base_mask.any()):
        return work

    group_columns = ["source", "station_key", "metric", "interval"]
    for _, group in work.loc[base_mask].groupby(group_columns, sort=False):
        idx = group.index
        observed = cast(pd.Series, work.loc[idx, "observed_at"])
        legacy_midnight_mask = (
            (observed.dt.hour == 23)
            & (observed.dt.minute == 59)
            & (observed.dt.second == 59)
            & (observed.dt.microsecond >= 999000)
        )
        if not bool(legacy_midnight_mask.any()):
            continue

        normalized = observed.copy()
        normalized.loc[legacy_midnight_mask] = normalized.loc[legacy_midnight_mask] + pd.Timedelta(microseconds=1)
        work.loc[idx, "observed_at"] = normalized

    return work


def _fill_period_columns_from_legacy(df: pd.DataFrame) -> pd.DataFrame:
    """period列が欠落した旧スキーマを契約スキーマへ補完する。"""

    if df.empty:
        return df
    work = df.copy()
    instantaneous_mask = (
        (work["source"] == "water_info")
        & work["metric"].isin(["water_level", "discharge"])
    )
    missing_end = work["period_end_at"].isna() & work["observed_at"].notna() & ~instantaneous_mask
    if bool(missing_end.any()):
        # hydrology_graphs 互換: 旧 observed_at は終端時刻として扱う。
        work.loc[missing_end, "period_end_at"] = work.loc[missing_end, "observed_at"]
    missing_start = work["period_start_at"].isna() & work["period_end_at"].notna()
    if bool(missing_start.any()):
        work.loc[missing_start, "period_start_at"] = work.loc[missing_start, "period_end_at"] - pd.to_timedelta(
            work.loc[missing_start, "interval"].map(_interval_hours), unit="h"
        )
    return work


def _infer_metric_candidates_from_filename(path: Path) -> tuple[str, ...]:
    """ファイル名からメトリクス候補を軽量推定する。"""

    stem = path.stem
    if stem.startswith("jma_"):
        return ("雨量",)
    if not stem.startswith("water_info_"):
        return ()

    remainder = stem[len("water_info_") :]
    parts = remainder.split("_")
    if len(parts) < 2:
        return ()

    metric_tokens = parts[1:]
    if len(metric_tokens) >= 2 and metric_tokens[0] == "water" and metric_tokens[1] == "level":
        metric = "water_level"
    else:
        metric = metric_tokens[0]
    label = _METRIC_LABELS.get(metric)
    if label is None:
        return ()
    return (label,)


def _normalize_metric_candidates_cell(value: object) -> tuple[str, ...]:
    """DataFrameセルに入った候補表現をタプルへ正規化する。"""

    if value is None:
        return ()
    if isinstance(value, tuple):
        items = value
    elif isinstance(value, list):
        items = tuple(value)
    elif isinstance(value, set):
        items = tuple(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        if text in _METRIC_LABELS.values():
            return (text,)
        return tuple(part.strip() for part in text.split("/") if part.strip())
    else:
        try:
            if pd.isna(value):
                return ()
        except Exception:  # noqa: BLE001
            pass
        items = (str(value),)

    normalized: list[str] = []
    seen: set[str] = set()
    for item in items:
        label = str(item).strip()
        if not label or label in seen:
            continue
        if label not in _METRIC_LABEL_ORDER:
            # 想定外の候補はそのまま残さず、既知ラベルだけを採用する。
            if label not in _METRIC_LABELS.values():
                continue
        seen.add(label)
        normalized.append(label)

    return tuple(sorted(normalized, key=_METRIC_LABEL_ORDER.index))


def _metric_label_for_metric(metric: object) -> str | None:
    """正規 metric 値を日本語ラベルへ変換する。"""

    text = str(metric or "").strip()
    return _METRIC_LABELS.get(text)


def _station_metric_labels_for_row(source: object, metric: object, metric_candidates: object | None) -> tuple[str, ...]:
    """1行分の候補ラベルを返す。"""

    source_text = str(source or "").strip()
    labels = list(_normalize_metric_candidates_cell(metric_candidates))
    if not labels:
        label = _metric_label_for_metric(metric)
        if label:
            labels.append(label)
    if source_text == "jma" and "雨量" not in labels:
        labels.insert(0, "雨量")
    normalized: list[str] = []
    seen: set[str] = set()
    for label in labels:
        if label in seen:
            continue
        seen.add(label)
        normalized.append(label)
    return tuple(sorted(normalized, key=_METRIC_LABEL_ORDER.index)) if normalized else ()


def _build_station_metric_labels(df: pd.DataFrame) -> dict[tuple[str, str], tuple[str, ...]]:
    """観測所ごとのメトリクス候補マップを構築する。"""

    if df.empty or "source" not in df.columns or "station_key" not in df.columns:
        return {}

    labels_by_pair: dict[tuple[str, str], set[str]] = {}
    has_metric = "metric" in df.columns
    has_candidates = "metric_candidates" in df.columns
    columns = ["source", "station_key"]
    if has_metric:
        columns.append("metric")
    if has_candidates:
        columns.append("metric_candidates")

    for row in df[columns].itertuples(index=False, name=None):
        source = row[0]
        station_key = row[1]
        metric = row[2] if has_metric else None
        metric_candidates = row[3] if has_candidates and has_metric else (row[2] if has_candidates else None)
        labels = _station_metric_labels_for_row(source, metric, metric_candidates)
        if not labels:
            continue
        pair = (str(source), str(station_key))
        labels_by_pair.setdefault(pair, set()).update(labels)

    return {
        pair: tuple(sorted(labels, key=_METRIC_LABEL_ORDER.index))
        for pair, labels in labels_by_pair.items()
    }


def _expand_legacy_schema(df: pd.DataFrame) -> pd.DataFrame:
    """旧来の広い列形式を、新スキーマの `metric/value/unit` に寄せる。"""

    work = df.copy()
    if "metric" in work.columns and "value" in work.columns and "unit" in work.columns:
        # すでに新スキーマなら、欠けている quality だけ補う。
        if "quality" not in work.columns:
            value_series = cast(pd.Series, pd.to_numeric(work["value"], errors="coerce"))
            work["quality"] = value_series.apply(lambda v: "missing" if pd.isna(v) else "normal")
        if "period_start_at" not in work.columns:
            work["period_start_at"] = pd.NaT
        if "period_end_at" not in work.columns:
            work["period_end_at"] = pd.NaT
        return work

    for column, metric, unit in _LEGACY_VALUE_COLUMNS:
        if column not in work.columns:
            continue
        # 旧列名は 1 つだけ使う。複数候補があっても最初に見つかったものを採用する。
        if "metric" not in work.columns:
            work["metric"] = metric
        if "unit" not in work.columns:
            work["unit"] = unit
        if "value" not in work.columns:
            work["value"] = cast(pd.Series, pd.to_numeric(work[column], errors="coerce"))
        if "quality" not in work.columns:
            value_series = cast(pd.Series, pd.to_numeric(work["value"], errors="coerce"))
            work["quality"] = value_series.apply(lambda v: "missing" if pd.isna(v) else "normal")
        if "period_start_at" not in work.columns:
            work["period_start_at"] = pd.NaT
        if "period_end_at" not in work.columns:
            work["period_end_at"] = pd.NaT
        break
    return work
