"""Excel writer helpers for water_info (xlsxwriter)."""

from __future__ import annotations

from typing import Iterable


def set_column_widths(worksheet, widths: dict[str, int]) -> None:
    for col, width in widths.items():
        if isinstance(col, str) and ":" in col:
            worksheet.set_column(col, width)
        else:
            worksheet.set_column(col, col, width)


def write_table(
    writer,
    sheet_name: str,
    df,
    column_widths: dict[str, int] | None = None,
    index: bool = False,
    startrow: int = 0,
    startcol: int = 0,
    columns: Iterable[str] | None = None,
):
    df.to_excel(
        writer,
        sheet_name=sheet_name,
        index=index,
        startrow=startrow,
        startcol=startcol,
        columns=columns,
    )
    ws = writer.sheets[sheet_name]
    if column_widths:
        set_column_widths(ws, column_widths)
    return ws


def add_scatter_chart(
    worksheet,
    workbook,
    sheet_name: str,
    max_row: int,
    x_col: int,
    y_col: int,
    name: str,
    insert_cell: str,
    x_axis: dict,
    y_axis: dict,
    title: str | None = None,
    size: tuple[int, int] = (720, 300),
):
    chart = workbook.add_chart({"type": "scatter", "subtype": "straight_with_markers"})
    chart.add_series(
        {
            "name": name,
            "categories": [sheet_name, 1, x_col, max_row - 1, x_col],
            "values": [sheet_name, 1, y_col, max_row - 1, y_col],
            "marker": {"type": "none"},
            "line": {"width": 1.5},
        }
    )
    if title:
        chart.set_title({"name": title})
    chart.set_x_axis(x_axis)
    chart.set_y_axis(y_axis)
    chart.set_legend({"position": "none"})
    chart.set_size({"width": size[0], "height": size[1]})
    worksheet.insert_chart(insert_cell, chart)
    return chart
