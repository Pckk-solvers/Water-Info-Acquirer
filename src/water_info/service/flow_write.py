"""Write flows for water_info."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..infra.date_utils import month_floor, shift_month
from ..infra.excel_summary import build_daily_empty_summary, build_year_summary
from ..infra.excel_writer import add_scatter_chart, set_column_widths, write_table


def write_hourly_excel(
    df,
    file_name: str | Path,
    value_col: str,
    mode_type: str,
    single_sheet: bool,
    empty_error_type: type[Exception] | None = None,
):
    if empty_error_type is not None:
        if df.empty or df.dropna(how="all").empty:
            raise empty_error_type("有効なデータがありません")

    with pd.ExcelWriter(file_name, engine="xlsxwriter", datetime_format="yyyy/m/d h:mm") as writer:
        if single_sheet:
            full_df = df[["display_dt", value_col]].copy()
            sheet_full = "全期間"
            ws_full = write_table(
                writer,
                sheet_full,
                full_df,
                column_widths={"A:A": 20, "B:B": 12},
            )
            max_row_full = len(full_df) + 1
            min_dt = full_df["display_dt"].min()
            max_dt = full_df["display_dt"].max()
            title_str = f"{min_dt.year}/{min_dt.month} - {max_dt.year}/{max_dt.month}"
            ytitle = {"S": "水位[m]", "R": "流量[m^3/s]", "U": "雨量[mm/h]"}[mode_type]
            xmin = shift_month(month_floor(min_dt), -1)
            xmax = shift_month(month_floor(max_dt), +2)

            add_scatter_chart(
                worksheet=ws_full,
                workbook=writer.book,
                sheet_name=sheet_full,
                max_row=max_row_full,
                x_col=0,
                y_col=1,
                name=sheet_full,
                insert_cell="D2",
                x_axis={
                    "name": "日時[月]",
                    "date_axis": True,
                    "num_format": "m",
                    "major_unit": 31,
                    "min": xmin,
                    "max": xmax,
                    "major_unit_type": "months",
                    "major_gridlines": {"visible": True},
                    "label_position": "low",
                },
                y_axis={"name": ytitle},
                title=title_str,
            )

        for year, group in df.groupby("sheet_year", sort=True):
            sheet_name = f"{year}年"
            ws = write_table(
                writer,
                sheet_name,
                group[["display_dt", value_col]],
                column_widths={"A:A": 20, "B:B": 12},
            )
            max_row = len(group) + 1
            gmin = group["display_dt"].min()
            gmax = group["display_dt"].max()
            xmin = shift_month(month_floor(gmin), -1)
            xmax = shift_month(month_floor(gmax), +2)
            ytitle = {"S": "水位[m]", "R": "流量[m^3/s]", "U": "雨量[mm/h]"}[mode_type]

            add_scatter_chart(
                worksheet=ws,
                workbook=writer.book,
                sheet_name=sheet_name,
                max_row=max_row,
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
            )

        daily_df = build_daily_empty_summary(df, value_col)
        year_summary_df = build_year_summary(df, value_col)

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

    return file_name


def write_daily_excel(
    df,
    file_name: str | Path,
    data_label: str,
    chart_title: str,
    single_sheet: bool,
):
    with pd.ExcelWriter(file_name, engine="xlsxwriter", datetime_format="yyyy/mm/dd") as writer:
        wb = writer.book
        if single_sheet:
            full_df = df.reset_index().rename(columns={"index": "datetime"})
            sheet_full = "全期間"
            ws_full = write_table(
                writer,
                sheet_full,
                full_df,
                column_widths={"A:A": 15, "B:B": 12},
                columns=["datetime", data_label],
            )
            max_row = len(full_df) + 1
            add_scatter_chart(
                worksheet=ws_full,
                workbook=wb,
                sheet_name=sheet_full,
                max_row=max_row,
                x_col=0,
                y_col=1,
                name=sheet_full,
                insert_cell="D6",
                x_axis={
                    "name": "日時[年/月]",
                    "date_axis": True,
                    "num_format": "yyyy/mm",
                    "major_unit": 185,
                    "major_gridlines": {"visible": True},
                },
                y_axis={"name": chart_title},
                title=f"{df.index.min().strftime('%Y/%m')} - {df.index.max().strftime('%Y/%m')}",
            )

        for year, grp in df.groupby(df.index.year):
            sheet = f"{year}年"
            grp_df = grp.reset_index().rename(columns={"index": "datetime"})
            ws = write_table(
                writer,
                sheet,
                grp_df,
                column_widths={"A:A": 15, "B:B": 12},
                columns=["datetime", data_label],
            )

            vals = grp_df[data_label]
            valid = vals.dropna()
            if not valid.empty:
                sheet_max_val = valid.max()
                sheet_max_date = grp_df.loc[valid.idxmax(), "datetime"].strftime("%Y/%m/%d")
                sheet_min_val = valid.min()
                sheet_min_date = grp_df.loc[valid.idxmin(), "datetime"].strftime("%Y/%m/%d")
                sheet_avg_val = valid.mean()
            else:
                sheet_max_date = sheet_min_date = sheet_avg_val = ""
                sheet_max_val = sheet_min_val = ""
            sheet_empty = int(vals.isna().sum())

            ws.write("D1", "シート最大値発生日")
            ws.write("E1", sheet_max_date)
            ws.write("F1", sheet_max_val)
            ws.write("D2", "シート最小値発生日")
            ws.write("E2", sheet_min_date)
            ws.write("F2", sheet_min_val)
            ws.write("D3", "シート平均値")
            ws.write("E3", sheet_avg_val)
            ws.write("D4", "シート空データ数")
            ws.write("E4", sheet_empty)
            set_column_widths(ws, {"D:D": 20, "E:E": 12, "F:F": 12})

            max_row = len(grp_df) + 1
            add_scatter_chart(
                worksheet=ws,
                workbook=wb,
                sheet_name=sheet,
                max_row=max_row,
                x_col=0,
                y_col=1,
                name=sheet,
                insert_cell="D6",
                x_axis={
                    "name": "日時[月]",
                    "date_axis": True,
                    "num_format": "mm",
                    "major_unit": 30,
                    "major_gridlines": {"visible": True},
                },
                y_axis={"name": chart_title},
            )

    return file_name
