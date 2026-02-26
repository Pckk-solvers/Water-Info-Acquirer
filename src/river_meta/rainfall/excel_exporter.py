from __future__ import annotations

from pathlib import Path

import pandas as pd

from .analysis import (
    ANNUAL_MAX_COLUMNS,
    STATION_SUMMARY_EXCEL_COLUMNS,
    STATION_SUMMARY_COLUMNS,
    TIMESERIES_COLUMNS,
    build_station_summary_dataframe,
)


RAINFALL_COLUMNS = [
    "1時間雨量(mm)",
    "3時間雨量(mm)",
    "6時間雨量(mm)",
    "12時間雨量(mm)",
    "24時間雨量(mm)",
    "48時間雨量(mm)",
    "最大雨量(mm)",
]

_SOURCE_LABEL_MAP = {
    "jma": "気象庁",
    "water_info": "水文水質DB",
}

# 年最大雨量一覧・時系列データからは除外するカラム (1ファイル=1観測所なので冗長)
_DROP_COLUMNS = ["データ元", "観測所キー", "観測所名"]


def export_station_rainfall_excel(
    timeseries_df: pd.DataFrame,
    annual_max_df: pd.DataFrame,
    *,
    output_path: str,
    decimal_places: int = 2,
) -> Path | None:
    if timeseries_df is None or timeseries_df.empty:
        return None

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    summary_internal = build_station_summary_dataframe(timeseries_df, annual_max_df).reindex(columns=STATION_SUMMARY_COLUMNS)
    summary_df = summary_internal.reindex(columns=STATION_SUMMARY_EXCEL_COLUMNS)
    annual_df = annual_max_df.reindex(columns=ANNUAL_MAX_COLUMNS).copy()
    timeseries = timeseries_df.reindex(columns=TIMESERIES_COLUMNS).copy()

    _round_numeric_columns(summary_df, decimal_places)
    _round_numeric_columns(annual_df, decimal_places)
    _round_numeric_columns(timeseries, decimal_places)

    # --- 表示用の変換 ---
    _apply_display_transforms(summary_df)
    _apply_display_transforms(annual_df)
    _apply_display_transforms(timeseries)

    # 年別サマリ: 欠測数を分母付きに (例: "123/8760")
    _format_missing_count_with_total(summary_df)

    # 年最大雨量一覧・時系列データ: 冗長カラムを除去
    annual_df = _drop_station_columns(annual_df)
    timeseries = _drop_station_columns(timeseries)

    output_exists = output.exists()

    with pd.ExcelWriter(
        output,
        engine="openpyxl",
        mode="a" if output_exists else "w",
        if_sheet_exists="overlay" if output_exists else None,
        date_format="YYYY/MM/DD HH:MM:SS",
    ) as writer:
        for sheet_name, data in (
            ("年別サマリ", summary_df),
            ("年最大雨量一覧", annual_df),
            ("時系列データ", timeseries),
        ):
            if output_exists and sheet_name in writer.sheets:
                startrow = writer.sheets[sheet_name].max_row
                data.to_excel(writer, sheet_name=sheet_name, index=False, header=False, startrow=startrow)
            else:
                data.to_excel(writer, sheet_name=sheet_name, index=False)

        # openpyxl engine auto-fit
        for sheet_name in ("年別サマリ", "年最大雨量一覧", "時系列データ"):
            if sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                _openpyxl_auto_fit_columns(ws)

    return output


def _apply_display_transforms(df: pd.DataFrame) -> None:
    """Excel 表示用の共通変換を適用する。"""
    # データ元: 英語 → 日本語
    if "データ元" in df.columns:
        df["データ元"] = df["データ元"].map(_SOURCE_LABEL_MAP).fillna(df["データ元"])

    # 観測所キー: ソースに応じて列名を変更
    if "観測所キー" in df.columns:
        # ソースを判定 (データ元列がある場合)
        source_raw = ""
        if "データ元" in df.columns and len(df) > 0:
            first_source = str(df["データ元"].iloc[0])
            # 日本語変換後の値で判定
            if first_source in ("気象庁", "jma"):
                source_raw = "jma"
            elif first_source in ("水文水質DB", "water_info"):
                source_raw = "water_info"

        # 値をstr型に統一
        df["観測所キー"] = df["観測所キー"].astype(str)

        if source_raw == "water_info":
            df.rename(columns={"観測所キー": "観測所記号"}, inplace=True)
        else:
            # JMA: アンダースコア → ハイフン
            df["観測所キー"] = df["観測所キー"].str.replace("_", "-", regex=False)
            df.rename(columns={"観測所キー": "地域番号-地点番号"}, inplace=True)

    # 年間完全性: bool → 完全/非完全
    if "年間完全性" in df.columns:
        df["年間完全性"] = df["年間完全性"].map({True: "完全", False: "非完全"}).fillna("非完全")

    # 観測時刻: datetime → 日付 + 時（1-24表記）
    if "観測時刻" in df.columns:
        dt_col = pd.to_datetime(df["観測時刻"], errors="coerce")
        col_idx = df.columns.get_loc("観測時刻")
        df.insert(col_idx, "日付", dt_col.dt.strftime("%Y/%m/%d"))
        df.insert(col_idx + 1, "時", dt_col.dt.hour + 1)
        df.drop(columns=["観測時刻"], inplace=True)

    # 発生日時: datetime → 発生日 + 発生時（1-24表記）
    if "発生日時" in df.columns:
        dt_col = pd.to_datetime(df["発生日時"], errors="coerce")
        col_idx = df.columns.get_loc("発生日時")
        df.insert(col_idx, "発生日", dt_col.dt.strftime("%Y/%m/%d"))
        df.insert(col_idx + 1, "発生時", dt_col.dt.hour + 1)
        df.drop(columns=["発生日時"], inplace=True)

    # N時間最大発生日時: 年別サマリ用（1時間〜48時間）
    for col_name in list(df.columns):
        if col_name.endswith("最大発生日時"):
            prefix = col_name.replace("最大発生日時", "")  # e.g. "1時間"
            dt_col = pd.to_datetime(df[col_name], errors="coerce")
            col_idx = df.columns.get_loc(col_name)
            df.insert(col_idx, f"{prefix}最大発生日", dt_col.dt.strftime("%Y/%m/%d"))
            df.insert(col_idx + 1, f"{prefix}最大発生時", dt_col.dt.hour + 1)
            df.drop(columns=[col_name], inplace=True)

    # 集計開始/集計終了: 年別サマリ用
    for col_name in ("集計開始", "集計終了"):
        if col_name in df.columns:
            dt_col = pd.to_datetime(df[col_name], errors="coerce")
            col_idx = df.columns.get_loc(col_name)
            df.insert(col_idx, f"{col_name}日", dt_col.dt.strftime("%Y/%m/%d"))
            df.insert(col_idx + 1, f"{col_name}時", dt_col.dt.hour + 1)
            df.drop(columns=[col_name], inplace=True)


def _format_missing_count_with_total(df: pd.DataFrame) -> None:
    """1時間欠測数を分母付きフォーマットに変換する (例: '123/8760')。"""
    if "1時間データ数" in df.columns and "1時間欠測数" in df.columns:
        total = pd.to_numeric(df["1時間データ数"], errors="coerce").fillna(0) + pd.to_numeric(df["1時間欠測数"], errors="coerce").fillna(0)
        missing = pd.to_numeric(df["1時間欠測数"], errors="coerce").fillna(0)
        df["1時間欠測数"] = missing.astype(int).astype(str) + "/" + total.astype(int).astype(str)


def _drop_station_columns(df: pd.DataFrame) -> pd.DataFrame:
    """1ファイル=1観測所のため冗長なカラムを除去する。"""
    cols_to_drop = [c for c in _DROP_COLUMNS if c in df.columns]
    # リネーム後の列名もチェック
    for col in ("地域番号-地点番号", "観測所記号"):
        if col in df.columns:
            cols_to_drop.append(col)
    return df.drop(columns=cols_to_drop, errors="ignore")


def _openpyxl_auto_fit_columns(worksheet) -> None:
    for col in worksheet.columns:
        max_length = 0
        column_letter = col[0].column_letter
        for cell in col:
            if cell.value:
                try:
                    line_len = max(len(str(line)) for line in str(cell.value).split('\n'))
                    if len(str(cell.value)) > max_length:
                        max_length = line_len
                except Exception:
                    pass
        adjusted_width = min(max((max_length + 2), 10), 40)
        worksheet.column_dimensions[column_letter].width = adjusted_width


def _round_numeric_columns(df: pd.DataFrame, decimal_places: int) -> None:
    for column in RAINFALL_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").round(decimal_places)
