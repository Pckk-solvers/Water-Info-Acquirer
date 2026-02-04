"""Unified entry points for water_info."""

from __future__ import annotations

from .service.flow_fetch import fetch_hourly_dataframe_for_code, fetch_daily_dataframe_for_code
from .service.flow_write import write_hourly_excel, write_daily_excel
from .infra.http_client import HEADERS, throttled_get
from .ui.app import show_water as _show_water


class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass


def process_data_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    df, file_name, value_col = fetch_hourly_dataframe_for_code(
        code=code,
        year_start=Y1,
        year_end=Y2,
        month_start=M1,
        month_end=M2,
        mode_type=mode_type,
        throttled_get=throttled_get,
        headers=HEADERS,
    )
    if df is None:
        return None

    if df.empty or df[value_col].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    write_hourly_excel(
        df=df,
        file_name=file_name,
        value_col=value_col,
        mode_type=mode_type,
        single_sheet=single_sheet,
        empty_error_type=EmptyExcelWarning,
    )

    print(f"Excelファイルの作成が完了しました。 {file_name}")
    return file_name


def process_period_date_display_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    """
    年単位URL（各年のBGNDATE=YYYY0101, ENDDATE=YYYY1231）を用いて指定年分のデータを取得し、
    開始月・終了月で指定された期間（例：2022/1～2023/9）にフィルタリング後、
    各シートにデータテーブル（A～C列）および追加統計情報（シート別・全体統計）を
    列D～Eに配置し、更に散布図をセル"D7"に配置するExcelファイルを出力します。

    追加：観測所コードに対応する観測所名をスクレイピングで取得し、ファイル名に挿入
    """
    df, file_name, data_label, chart_title = fetch_daily_dataframe_for_code(
        code=code,
        year_start=Y1,
        year_end=Y2,
        month_start=M1,
        month_end=M2,
        mode_type=mode_type,
        throttled_get=throttled_get,
        headers=HEADERS,
    )
    if df is None:
        print("mode_typeは 'S', 'R', または 'U' を指定してください。")
        return None

    if df.empty or df[data_label].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    write_daily_excel(
        df=df,
        file_name=file_name,
        data_label=data_label,
        chart_title=chart_title,
        single_sheet=single_sheet,
    )

    print(f"生成完了: {file_name}")
    return file_name


def show_water(parent, single_sheet_mode=False, on_open_other=None, on_close=None):
    """Factory for launcher to create water_info window."""
    return _show_water(
        parent=parent,
        fetch_hourly=process_data_for_code,
        fetch_daily=process_period_date_display_for_code,
        empty_error_type=EmptyExcelWarning,
        single_sheet_mode=single_sheet_mode,
        on_open_other=on_open_other,
        on_close=on_close,
    )
