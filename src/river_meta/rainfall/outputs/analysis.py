from __future__ import annotations
# pyright: reportArgumentType=false, reportOptionalMemberAccess=false, reportAttributeAccessIssue=false, reportCallIssue=false, reportGeneralTypeIssues=false

from datetime import datetime

import pandas as pd


ROLLING_COLUMN_MAP = {
    3: "3時間雨量(mm)",
    6: "6時間雨量(mm)",
    12: "12時間雨量(mm)",
    24: "24時間雨量(mm)",
    48: "48時間雨量(mm)",
}

TIMESERIES_COLUMNS = [
    "データ元",
    "観測所キー",
    "観測所名",
    "観測時刻",
    "1時間雨量(mm)",
    "3時間雨量(mm)",
    "6時間雨量(mm)",
    "12時間雨量(mm)",
    "24時間雨量(mm)",
    "48時間雨量(mm)",
    "品質",
]

ANNUAL_MAX_COLUMNS = [
    "データ元",
    "観測所キー",
    "観測所名",
    "年",
    "指標",
    "最大雨量(mm)",
    "発生日時",
    "年間完全性",
    "備考",
]

STATION_SUMMARY_COLUMNS = [
    "データ元",
    "観測所キー",
    "観測所名",
    "西暦",
    "和暦",
    "集計開始",
    "集計終了",
    "1時間データ数",
    "1時間欠測数",
    "1時間最大雨量(mm)",
    "1時間最大発生日時",
    "3時間最大雨量(mm)",
    "3時間最大発生日時",
    "6時間最大雨量(mm)",
    "6時間最大発生日時",
    "12時間最大雨量(mm)",
    "12時間最大発生日時",
    "24時間最大雨量(mm)",
    "24時間最大発生日時",
    "48時間最大雨量(mm)",
    "48時間最大発生日時",
    "年間完全性",
    "備考",
]

STATION_SUMMARY_EXCEL_COLUMNS = [
    "データ元",
    "観測所キー",
    "観測所名",
    "西暦",
    "和暦",
    "1時間データ数",
    "1時間欠測数",
    "1時間最大雨量(mm)",
    "1時間最大発生日時",
    "3時間最大雨量(mm)",
    "3時間最大発生日時",
    "6時間最大雨量(mm)",
    "6時間最大発生日時",
    "12時間最大雨量(mm)",
    "12時間最大発生日時",
    "24時間最大雨量(mm)",
    "24時間最大発生日時",
    "48時間最大雨量(mm)",
    "48時間最大発生日時",
]


def year_to_japanese_era(year: int) -> str:
    if year >= 2019:
        return f"R{year - 2018}"
    if year >= 1989:
        return f"H{year - 1988}"
    if year >= 1926:
        return f"S{year - 1925}"
    if year >= 1912:
        return f"T{year - 1911}"
    if year >= 1868:
        return f"M{year - 1867}"
    return ""


def build_hourly_timeseries_dataframe(records_df: pd.DataFrame) -> pd.DataFrame:
    if records_df is None or records_df.empty:
        return pd.DataFrame(columns=TIMESERIES_COLUMNS)

    prepared = records_df.copy()
    # 新スキーマ (metric/value) 入力も受け付ける。時系列雨量集計では rainfall のみ対象。
    if "rainfall_mm" not in prepared.columns and {"metric", "value"}.issubset(set(prepared.columns)):
        metric_series = prepared.get("metric", pd.Series([""] * len(prepared), index=prepared.index)).astype(str)
        value_series = pd.to_numeric(prepared.get("value"), errors="coerce")
        prepared = prepared[metric_series.eq("rainfall")].copy()
        prepared["rainfall_mm"] = value_series.loc[prepared.index]

    prepared["period_end_at"] = pd.to_datetime(prepared.get("period_end_at"), errors="coerce")
    prepared["observed_at"] = pd.to_datetime(prepared.get("observed_at"), errors="coerce")
    prepared["time_at"] = prepared["period_end_at"].where(prepared["period_end_at"].notna(), prepared["observed_at"])
    prepared = prepared[prepared["interval"].astype(str).eq("1hour")]
    prepared = prepared.dropna(subset=["time_at"])
    if prepared.empty:
        return pd.DataFrame(columns=TIMESERIES_COLUMNS)

    prepared["rainfall_mm"] = pd.to_numeric(prepared.get("rainfall_mm"), errors="coerce")
    prepared["quality"] = prepared.get("quality", "normal")

    rows: list[pd.DataFrame] = []
    group_cols = ["source", "station_key", "station_name"]
    for (source, station_key, station_name), group in prepared.groupby(group_cols, sort=False):
        station_df = _build_station_timeseries(group, source, station_key, station_name)
        rows.append(station_df)

    merged = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=TIMESERIES_COLUMNS)
    if merged.empty:
        return pd.DataFrame(columns=TIMESERIES_COLUMNS)
    return merged[TIMESERIES_COLUMNS].sort_values(["観測所キー", "観測時刻"]).reset_index(drop=True)


def _build_station_timeseries(
    station_records: pd.DataFrame,
    source: str,
    station_key: str,
    station_name: str,
) -> pd.DataFrame:
    station_records = station_records.sort_values("time_at").drop_duplicates(subset=["time_at"])
    min_at = station_records["time_at"].min().floor("h")
    max_at = station_records["time_at"].max().floor("h")
    full_index = pd.date_range(min_at, max_at, freq="h")

    aligned = station_records.set_index("time_at").reindex(full_index)
    aligned["source"] = source
    aligned["station_key"] = station_key
    aligned["station_name"] = station_name
    aligned["rainfall_mm"] = pd.to_numeric(aligned["rainfall_mm"], errors="coerce")
    aligned["quality"] = aligned.get("quality")
    aligned.loc[aligned["quality"].isna() & aligned["rainfall_mm"].isna(), "quality"] = "missing"
    aligned.loc[aligned["quality"].isna() & aligned["rainfall_mm"].notna(), "quality"] = "normal"

    out = pd.DataFrame(
        {
            "データ元": aligned["source"],
            "観測所キー": aligned["station_key"],
            "観測所名": aligned["station_name"],
            "観測時刻": full_index,
            "1時間雨量(mm)": aligned["rainfall_mm"],
            "品質": aligned["quality"].map({"normal": "正常", "missing": "欠測"}).fillna(aligned["quality"]),
        }
    )
    for hours, column in ROLLING_COLUMN_MAP.items():
        out[column] = out["1時間雨量(mm)"].rolling(window=hours, min_periods=hours).sum()
    return out


def build_annual_max_dataframe(timeseries_df: pd.DataFrame) -> pd.DataFrame:
    if timeseries_df is None or timeseries_df.empty:
        return pd.DataFrame(columns=ANNUAL_MAX_COLUMNS)

    required = {"観測時刻", "観測所キー", "観測所名", "データ元", "1時間雨量(mm)"}
    missing = required - set(timeseries_df.columns)
    if missing:
        raise ValueError(f"timeseries_df is missing required columns: {sorted(missing)}")

    frame = timeseries_df.copy()
    frame["観測時刻"] = pd.to_datetime(frame["観測時刻"], errors="coerce")
    frame = frame.dropna(subset=["観測時刻"])
    if frame.empty:
        return pd.DataFrame(columns=ANNUAL_MAX_COLUMNS)

    rows: list[dict[str, object]] = []
    metric_columns = ["1時間雨量(mm)"] + list(ROLLING_COLUMN_MAP.values())

    for (source, station_key, station_name), group in frame.groupby(["データ元", "観測所キー", "観測所名"], sort=False):
        indexed = (
            group.set_index("観測時刻")
            .sort_index()
            .loc[:, metric_columns]
            .apply(pd.to_numeric, errors="coerce")
        )
        for year in sorted(indexed.index.year.unique()):
            start = datetime(year, 1, 1, 0, 0, 0)
            end = datetime(year, 12, 31, 23, 0, 0)
            full_index = pd.date_range(start, end, freq="h")

            hourly = indexed["1時間雨量(mm)"].reindex(full_index)
            is_complete = bool(hourly.notna().all())
            note = "" if is_complete else "参考値（欠測あり）"

            for column in metric_columns:
                yearly_series = indexed[column].reindex(full_index)
                valid = yearly_series.dropna()
                if valid.empty:
                    max_value = None
                    occurred_at = pd.NaT
                else:
                    max_value = float(valid.max())
                    occurred_at = valid[valid.eq(max_value)].index.min()

                rows.append(
                    {
                        "データ元": source,
                        "観測所キー": station_key,
                        "観測所名": station_name,
                        "年": year,
                        "指標": column.replace("(mm)", ""),
                        "最大雨量(mm)": max_value,
                        "発生日時": occurred_at,
                        "年間完全性": is_complete,
                        "備考": note,
                    }
                )

    result = pd.DataFrame(rows, columns=ANNUAL_MAX_COLUMNS)
    if result.empty:
        return pd.DataFrame(columns=ANNUAL_MAX_COLUMNS)
    result["発生日時"] = pd.to_datetime(result["発生日時"], errors="coerce")
    # 指標を定義順（1h, 3h, 6h, 12h, 24h, 48h）でソート
    metric_order = [c.replace("(mm)", "") for c in metric_columns]
    result["指標"] = pd.Categorical(result["指標"], categories=metric_order, ordered=True)
    return result.sort_values(["観測所キー", "年", "指標"]).reset_index(drop=True)


def build_station_summary_dataframe(timeseries_df: pd.DataFrame, annual_max_df: pd.DataFrame) -> pd.DataFrame:
    if timeseries_df is None or timeseries_df.empty:
        return pd.DataFrame(columns=STATION_SUMMARY_COLUMNS)

    rows: list[dict[str, object]] = []
    metrics = [
        ("1時間", "1時間雨量(mm)"),
        ("3時間", "3時間雨量(mm)"),
        ("6時間", "6時間雨量(mm)"),
        ("12時間", "12時間雨量(mm)"),
        ("24時間", "24時間雨量(mm)"),
        ("48時間", "48時間雨量(mm)"),
    ]

    for (source, station_key, station_name), group in timeseries_df.groupby(
        ["データ元", "観測所キー", "観測所名"],
        sort=False,
    ):
        station = group.sort_values("観測時刻").copy()
        station["観測時刻"] = pd.to_datetime(station["観測時刻"], errors="coerce")
        station = station.dropna(subset=["観測時刻"])
        if station.empty:
            continue

        station_indexed = station.set_index("観測時刻").sort_index()
        station_annual = annual_max_df[
            (annual_max_df["データ元"] == source)
            & (annual_max_df["観測所キー"] == station_key)
            & (annual_max_df["観測所名"] == station_name)
        ].copy()
        candidate_years = sorted(
            {
                int(year)
                for year in list(station_indexed.index.year.unique()) + station_annual["年"].dropna().tolist()
                if str(year).isdigit()
            }
        )

        for year in candidate_years:
            year_start = datetime(year, 1, 1, 0, 0, 0)
            year_end = datetime(year, 12, 31, 23, 0, 0)
            full_index = pd.date_range(year_start, year_end, freq="h")
            station_year = station_indexed.reindex(full_index)
            actual_year_rows = station[station["観測時刻"].dt.year == year]
            hourly = pd.to_numeric(station_year["1時間雨量(mm)"], errors="coerce")
            annual_rows = station_annual[station_annual["年"] == year]

            row: dict[str, object] = {
                "データ元": source,
                "観測所キー": station_key,
                "観測所名": station_name,
                "西暦": year,
                "和暦": year_to_japanese_era(year),
                "集計開始": actual_year_rows["観測時刻"].min(),
                "集計終了": actual_year_rows["観測時刻"].max(),
                "1時間データ数": int(hourly.notna().sum()),
                "1時間欠測数": int(hourly.isna().sum()),
            }

            for label, _column in metrics:
                metric_name = f"{label}雨量"
                metric_row = annual_rows[annual_rows["指標"] == metric_name]
                if metric_row.empty:
                    row[f"{label}最大雨量(mm)"] = None
                    row[f"{label}最大発生日時"] = pd.NaT
                    continue
                metric_first = metric_row.iloc[0]
                row[f"{label}最大雨量(mm)"] = metric_first["最大雨量(mm)"]
                row[f"{label}最大発生日時"] = metric_first["発生日時"]

            base_metric = annual_rows[annual_rows["指標"] == "1時間雨量"]
            if base_metric.empty:
                is_complete = False
                note = "参考値（欠測あり）"
            else:
                base_first = base_metric.iloc[0]
                is_complete = bool(base_first["年間完全性"])
                note = str(base_first["備考"] or "")
            row["年間完全性"] = is_complete
            row["備考"] = note
            rows.append(row)

    summary = pd.DataFrame(rows, columns=STATION_SUMMARY_COLUMNS)
    if summary.empty:
        return pd.DataFrame(columns=STATION_SUMMARY_COLUMNS)

    datetime_columns = [c for c in summary.columns if c.endswith("発生日時")] + ["集計開始", "集計終了"]
    for col in datetime_columns:
        summary[col] = pd.to_datetime(summary[col], errors="coerce")
    return summary.sort_values(["観測所キー", "西暦"]).reset_index(drop=True)
