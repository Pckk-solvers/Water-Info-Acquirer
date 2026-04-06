"""Write flows for water_info."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, cast

import pandas as pd

from ..infra.date_utils import month_floor, shift_month
from ..infra.excel_summary import build_daily_empty_summary, build_year_summary
from ..infra.excel_writer import add_scatter_chart, set_column_widths, write_table

_SOURCE_SHEET = "出典"


def _resolve_excel_display_at(df: pd.DataFrame) -> pd.Series:
    """Excel表示用時刻を解決する。"""

    if "period_end_at" in df.columns:
        resolved = pd.to_datetime(df["period_end_at"], errors="coerce")
        if not resolved.isna().all():
            return resolved
    if "observed_at" in df.columns:
        resolved = pd.to_datetime(df["observed_at"], errors="coerce")
        if not resolved.isna().all():
            return resolved
    if "datetime" in df.columns:
        return pd.to_datetime(df["datetime"], errors="coerce")
    return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")


def _write_source_sheet(writer, source_info: dict) -> None:
    ws = writer.book.add_worksheet(_SOURCE_SHEET)
    writer.sheets[_SOURCE_SHEET] = ws
    rows = [
        ("出典", source_info.get("source_name", "")),
        ("URL", source_info.get("source_url", "")),
        ("取得日", source_info.get("retrieved_at", datetime.now().strftime("%Y-%m-%d %H:%M"))),
        ("観測所名", source_info.get("station_name", "")),
        ("観測所コード", source_info.get("station_code", "")),
        ("取得期間(開始)", source_info.get("period_start", "")),
        ("取得期間(終了)", source_info.get("period_end", "")),
        ("取得項目", source_info.get("item", "")),
        ("データ種別", source_info.get("data_kind", "")),
        ("URLログ", source_info.get("url_log", "")),
        ("出力ファイル名", source_info.get("output_file", "")),
        ("取得条件概要", source_info.get("summary", "")),
    ]
    for idx, (label, value) in enumerate(rows):
        ws.write(idx, 0, label)
        ws.write(idx, 1, value)
    set_column_widths(ws, {"A:A": 12, "B:B": 60})


def _add_hourly_sheet_with_chart(
    *,
    writer: pd.ExcelWriter,
    sheet_name: str,
    sheet_df: pd.DataFrame,
    value_col: str,
    mode_type: str,
    title: str | None = None,
) -> None:
    ws = write_table(
        writer,
        sheet_name,
        sheet_df,
        column_widths={"A:A": 20, "B:B": 12},
    )
    if sheet_df.empty:
        return
    dt_series = pd.to_datetime(sheet_df["datetime"], errors="coerce")
    min_dt = dt_series.min()
    max_dt = dt_series.max()
    if pd.isna(min_dt) or pd.isna(max_dt):
        return
    min_ts = pd.Timestamp(min_dt)
    max_ts = pd.Timestamp(max_dt)
    min_dt_value = cast(datetime, min_ts.to_pydatetime())
    max_dt_value = cast(datetime, max_ts.to_pydatetime())
    xmin = shift_month(month_floor(min_dt_value), -1)
    xmax = shift_month(month_floor(max_dt_value), +2)
    ytitle = {"S": "水位[m]", "R": "流量[m^3/s]", "U": "雨量[mm/h]"}[mode_type]
    add_scatter_chart(
        worksheet=ws,
        workbook=writer.book,
        sheet_name=sheet_name,
        max_row=len(sheet_df) + 1,
        x_col=0,
        y_col=1,
        name=sheet_name,
        insert_cell="D2",
        x_axis={
            "name": "日時[月]",
            "date_axis": True,
            "num_format": "m",
            "major_unit": 31,
            "major_unit_type": "months",
            "min": xmin,
            "max": xmax,
            "major_gridlines": {"visible": True},
            "label_position": "low",
        },
        y_axis={"name": ytitle},
        title=title,
    )


def _add_daily_sheet_with_chart(
    *,
    writer: pd.ExcelWriter,
    sheet_name: str,
    sheet_df: pd.DataFrame,
    data_label: str,
    chart_title: str,
    title: str | None = None,
    stats: dict[str, Any] | None = None,
) -> None:
    ws = write_table(
        writer,
        sheet_name,
        sheet_df,
        column_widths={"A:A": 15, "B:B": 12},
        columns=["datetime", data_label],
    )
    if stats is not None:
        ws.write("D1", "シート最大値発生日")
        ws.write("E1", stats.get("max_date", ""))
        ws.write("F1", stats.get("max_val", ""))
        ws.write("D2", "シート最小値発生日")
        ws.write("E2", stats.get("min_date", ""))
        ws.write("F2", stats.get("min_val", ""))
        ws.write("D3", "シート平均値")
        ws.write("E3", stats.get("avg_val", ""))
        ws.write("D4", "シート空データ数")
        ws.write("E4", stats.get("empty_count", 0))
        set_column_widths(ws, {"D:D": 20, "E:E": 12, "F:F": 12})
    if sheet_df.empty:
        return
    dt_series = pd.to_datetime(sheet_df["datetime"], errors="coerce")
    min_dt = dt_series.min()
    max_dt = dt_series.max()
    if pd.isna(min_dt) or pd.isna(max_dt):
        return
    min_ts = pd.Timestamp(min_dt)
    max_ts = pd.Timestamp(max_dt)
    min_axis = shift_month(month_floor(cast(datetime, min_ts.to_pydatetime())), -1)
    max_axis = shift_month(month_floor(cast(datetime, max_ts.to_pydatetime())), +2)
    add_scatter_chart(
        worksheet=ws,
        workbook=writer.book,
        sheet_name=sheet_name,
        max_row=len(sheet_df) + 1,
        x_col=0,
        y_col=1,
        name=sheet_name,
        insert_cell="D6",
        x_axis={
            "name": "日時[月]" if title is None else "日時[年/月]",
            "date_axis": True,
            "num_format": "mm" if title is None else "yyyy/mm",
            "major_unit": 1,
            "major_unit_type": "months",
            "min": min_axis,
            "max": max_axis,
            "major_gridlines": {"visible": True},
        },
        y_axis={"name": chart_title},
        title=title,
    )


def write_hourly_excel(
    df,
    file_name: str | Path,
    value_col: str,
    mode_type: str,
    single_sheet: bool,
    source_info: dict | None = None,
    empty_error_type: type[Exception] | None = None,
):
    if empty_error_type is not None:
        if df.empty or df.dropna(how="all").empty:
            raise empty_error_type("有効なデータがありません")

    with pd.ExcelWriter(file_name, engine="xlsxwriter", datetime_format="yyyy/m/d h:mm") as writer:
        excel_display_at = _resolve_excel_display_at(df)
        work_df = df.copy()
        target_sheets: list[tuple[str, pd.DataFrame, str | None]] = []
        if single_sheet:
            full_df = pd.DataFrame({"datetime": excel_display_at, value_col: work_df[value_col]})
            min_dt = pd.to_datetime(full_df["datetime"], errors="coerce").min()
            max_dt = pd.to_datetime(full_df["datetime"], errors="coerce").max()
            title_str: str | None = None
            if not pd.isna(min_dt) and not pd.isna(max_dt):
                min_ts = pd.Timestamp(min_dt)
                max_ts = pd.Timestamp(max_dt)
                title_str = f"{min_ts.year}/{min_ts.month} - {max_ts.year}/{max_ts.month}"
            target_sheets.append(("全期間", full_df, title_str))

        sheet_year = (
            pd.to_numeric(work_df["sheet_year"], errors="coerce")
            if "sheet_year" in work_df.columns
            else pd.to_datetime(excel_display_at, errors="coerce").dt.year
        )
        sheet_year_series = pd.Series(sheet_year, index=work_df.index)
        display_series = pd.Series(excel_display_at, index=work_df.index)
        for year in sorted(int(y) for y in sheet_year_series.dropna().unique()):
            group_mask = sheet_year_series.eq(year)
            group = work_df.loc[group_mask].copy()
            sheet_name = f"{year}年"
            sheet_df = pd.DataFrame(
                {
                    "datetime": display_series.loc[group.index].reset_index(drop=True),
                    value_col: group[value_col].reset_index(drop=True),
                }
            )
            target_sheets.append((sheet_name, sheet_df, None))
        for sheet_name, sheet_df, title in target_sheets:
            _add_hourly_sheet_with_chart(
                writer=writer,
                sheet_name=sheet_name,
                sheet_df=sheet_df,
                value_col=value_col,
                mode_type=mode_type,
                title=title,
            )

        summary_df = work_df.copy()
        summary_df["__excel_display_at"] = excel_display_at
        daily_df = build_daily_empty_summary(summary_df, value_col, time_col="__excel_display_at")
        year_summary_df = build_year_summary(summary_df, value_col, time_col="__excel_display_at")

        ws = write_table(
            writer,
            "summary",
            daily_df,
            column_widths={"A:A": 15, "B:B": 12},
            startrow=0,
            startcol=0,
        )

        write_table(
            writer,
            "summary",
            year_summary_df,
            startrow=0,
            startcol=3,
        )
        set_column_widths(ws, {"D:D": 8, "E:E": 20, "F:F": 10, "G:G": 18})
        _write_source_sheet(writer, source_info or {})

    return file_name


def write_daily_excel(
    df,
    file_name: str | Path,
    data_label: str,
    chart_title: str,
    single_sheet: bool,
    source_info: dict | None = None,
):
    with pd.ExcelWriter(file_name, engine="xlsxwriter", datetime_format="yyyy/mm/dd") as writer:
        target_sheets: list[tuple[str, pd.DataFrame, str | None, dict[str, Any] | None]] = []

        if single_sheet:
            full_df = df.reset_index().rename(columns={"index": "datetime"})
            title = f"{df.index.min().strftime('%Y/%m')} - {df.index.max().strftime('%Y/%m')}"
            target_sheets.append(("全期間", full_df, title, None))

        for year, grp in df.groupby(df.index.year):
            sheet = f"{year}年"
            grp_df = grp.reset_index().rename(columns={"index": "datetime"})
            vals = grp_df[data_label]
            valid = vals.dropna()
            if not valid.empty:
                stats = {
                    "max_val": valid.max(),
                    "max_date": grp_df.loc[valid.idxmax(), "datetime"].strftime("%Y/%m/%d"),
                    "min_val": valid.min(),
                    "min_date": grp_df.loc[valid.idxmin(), "datetime"].strftime("%Y/%m/%d"),
                    "avg_val": valid.mean(),
                    "empty_count": int(vals.isna().sum()),
                }
            else:
                stats = {
                    "max_val": "",
                    "max_date": "",
                    "min_val": "",
                    "min_date": "",
                    "avg_val": "",
                    "empty_count": int(vals.isna().sum()),
                }
            target_sheets.append((sheet, grp_df, None, stats))

        for sheet_name, sheet_df, title, stats in target_sheets:
            _add_daily_sheet_with_chart(
                writer=writer,
                sheet_name=sheet_name,
                sheet_df=sheet_df,
                data_label=data_label,
                chart_title=chart_title,
                title=title,
                stats=stats,
            )
        _write_source_sheet(writer, source_info or {})

    return file_name
