"""Unified entry points for water_info."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .service.flow_fetch import fetch_hourly_dataframe_for_code, fetch_daily_dataframe_for_code
from .service.flow_write import write_hourly_excel, write_daily_excel
from .infra.http_client import HEADERS, throttled_get
from .infra.url_builder import build_daily_base_url, build_hourly_base
from .ui.app import show_water as _show_water


class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass


_UNIFIED_COLUMNS = [
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
]


def _save_unified_records_parquet(records: list[dict], output_path: Path) -> Path:
    df = pd.DataFrame(records, columns=_UNIFIED_COLUMNS)
    if df.empty:
        df = pd.DataFrame(columns=_UNIFIED_COLUMNS)
    df["period_start_at"] = pd.to_datetime(df.get("period_start_at"), errors="coerce")
    df["period_end_at"] = pd.to_datetime(df.get("period_end_at"), errors="coerce")
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    return output_path


def _resolve_gui_output_path(file_name: str | Path) -> Path:
    """water_info GUI のExcel出力先を outputs/water_info に固定する。"""
    out_dir = Path("outputs") / "water_info"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / Path(file_name).name


def _resolve_gui_parquet_dir() -> Path:
    out_dir = Path("outputs") / "water_info" / "parquet"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _safe_token(value: str) -> str:
    return str(value).replace("/", "_").replace("\\", "_")


def _metric_and_unit(mode_type: str) -> tuple[str, str]:
    if mode_type == "S":
        return "water_level", "m"
    if mode_type == "R":
        return "discharge", "m3/s"
    return "rainfall", "mm"


def _build_water_info_parquet_path(
    *,
    code: str,
    mode_type: str,
    interval: str,
    year_start: str,
    month_start: str,
    year_end: str,
    month_end: str,
) -> Path:
    metric, _ = _metric_and_unit(mode_type)
    month_start_num = int(str(month_start).replace("月", ""))
    month_end_num = int(str(month_end).replace("月", ""))
    period_start = f"{year_start}{month_start_num:02d}"
    period_end = f"{year_end}{month_end_num:02d}"
    name = (
        f"water_info_{_safe_token(code)}_{metric}_{interval}_"
        f"{period_start}_{period_end}.parquet"
    )
    return _resolve_gui_parquet_dir() / name


def _export_water_info_parquet(
    *,
    df: pd.DataFrame,
    code: str,
    station_name: str,
    mode_type: str,
    interval: str,
    year_start: str,
    month_start: str,
    year_end: str,
    month_end: str,
    value_col: str,
) -> Path:
    metric, unit = _metric_and_unit(mode_type)
    station_key = str(code)
    out_path = _build_water_info_parquet_path(
        code=code,
        mode_type=mode_type,
        interval=interval,
        year_start=year_start,
        month_start=month_start,
        year_end=year_end,
        month_end=month_end,
    )

    rows = []
    if interval == "1day":
        date_idx = df.index if "datetime" not in df.columns else pd.to_datetime(df["datetime"], errors="coerce")
        for idx, value_raw in zip(date_idx, df[value_col]):
            observed_at = pd.to_datetime(idx, errors="coerce")
            if pd.isna(observed_at):
                continue
            value = pd.to_numeric(value_raw, errors="coerce")
            value_float = None if pd.isna(value) else float(value)
            period_start_at = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end_at = period_start_at + pd.Timedelta(days=1)
            rows.append(
                {
                    "source": "water_info",
                    "station_key": station_key,
                    "station_name": station_name or "",
                    "period_start_at": period_start_at.to_pydatetime(),
                    "period_end_at": period_end_at.to_pydatetime(),
                    "observed_at": period_end_at.to_pydatetime(),
                    "metric": metric,
                    "value": value_float,
                    "unit": unit,
                    "interval": interval,
                    "quality": "normal" if value_float is not None else "missing",
                }
            )
    else:
        # 時刻契約は datetime 列を正とする。
        datetime_col = df.get("datetime")
        datetimes = pd.to_datetime(datetime_col, errors="coerce") if datetime_col is not None else None
        if datetimes is None:
            datetimes = pd.Series(dtype="datetime64[ns]")
        for period_start_at, value_raw in zip(datetimes, df[value_col]):
            if pd.isna(period_start_at):
                continue
            period_end_at = period_start_at + pd.Timedelta(hours=1)
            value = pd.to_numeric(value_raw, errors="coerce")
            value_float = None if pd.isna(value) else float(value)
            rows.append(
                {
                    "source": "water_info",
                    "station_key": station_key,
                    "station_name": station_name or "",
                    "period_start_at": period_start_at.to_pydatetime(),
                    "period_end_at": period_end_at.to_pydatetime(),
                    "observed_at": period_end_at.to_pydatetime(),
                    "metric": metric,
                    "value": value_float,
                    "unit": unit,
                    "interval": interval,
                    "quality": "normal" if value_float is not None else "missing",
                }
            )

    return _save_unified_records_parquet(rows, out_path)


def source_info_item_label(mode_type: str) -> str:
    return {"S": "水位", "R": "流量", "U": "雨量"}[mode_type]


def process_data_for_code(
    code,
    Y1,
    Y2,
    M1,
    M2,
    mode_type,
    single_sheet=False,
    export_parquet=False,
    progress_callback=None,
):
    df, file_name, value_col = fetch_hourly_dataframe_for_code(
        code=code,
        year_start=Y1,
        year_end=Y2,
        month_start=M1,
        month_end=M2,
        mode_type=mode_type,
        throttled_get=throttled_get,
        headers=HEADERS,
        progress_callback=progress_callback,
    )
    if df is None:
        return None

    if df.empty or df[value_col].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    _, mode_str = build_hourly_base(mode_type)
    station_name = file_name.name.split("_")[1] if file_name else ""
    output_path = _resolve_gui_output_path(file_name if file_name else f"{code}.xlsx")
    source_info = {
        "source_name": "国土交通省 水文水質データベース",
        "source_url": f"http://www1.river.go.jp/cgi-bin/Dsp{mode_str}Data.exe",
        "station_name": station_name,
        "station_code": code,
        "period": f"{Y1}/{M1}-{Y2}/{M2}",
        "period_start": f"{Y1}/{M1}",
        "period_end": f"{Y2}/{M2}",
        "item": source_info_item_label(mode_type),
        "data_kind": "時刻",
        "url_log": "コンソール出力",
        "output_file": output_path.name,
        "summary": f"{station_name}({code}) {Y1}/{M1}-{Y2}/{M2} {source_info_item_label(mode_type)}",
    }
    write_hourly_excel(
        df=df,
        file_name=output_path,
        value_col=value_col,
        mode_type=mode_type,
        single_sheet=single_sheet,
        source_info=source_info,
        empty_error_type=EmptyExcelWarning,
    )
    if export_parquet:
        parquet_path = _export_water_info_parquet(
            df=df,
            code=str(code),
            station_name=station_name,
            mode_type=mode_type,
            interval="1hour",
            year_start=str(Y1),
            month_start=str(M1),
            year_end=str(Y2),
            month_end=str(M2),
            value_col=value_col,
        )
        print(f"Parquetファイルを出力しました。 {parquet_path}")

    print(f"Excelファイルの作成が完了しました。 {output_path}")
    return output_path


def process_period_date_display_for_code(
    code,
    Y1,
    Y2,
    M1,
    M2,
    mode_type,
    single_sheet=False,
    export_parquet=False,
    progress_callback=None,
):
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
        progress_callback=progress_callback,
    )
    if df is None:
        print("mode_typeは 'S', 'R', または 'U' を指定してください。")
        return None

    if df.empty or df[data_label].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    station_name = file_name.name.split("_")[1] if file_name else ""
    output_path = _resolve_gui_output_path(file_name if file_name else f"{code}.xlsx")
    source_info = {
        "source_name": "国土交通省 水文水質データベース",
        "source_url": build_daily_base_url(mode_type),
        "station_name": station_name,
        "station_code": code,
        "period": f"{Y1}/{M1}-{Y2}/{M2}",
        "period_start": f"{Y1}/{M1}",
        "period_end": f"{Y2}/{M2}",
        "item": source_info_item_label(mode_type),
        "data_kind": "日",
        "url_log": "コンソール出力",
        "output_file": output_path.name,
        "summary": f"{station_name}({code}) {Y1}/{M1}-{Y2}/{M2} {source_info_item_label(mode_type)}",
    }
    write_daily_excel(
        df=df,
        file_name=output_path,
        data_label=data_label,
        chart_title=chart_title,
        single_sheet=single_sheet,
        source_info=source_info,
    )
    if export_parquet:
        parquet_path = _export_water_info_parquet(
            df=df,
            code=str(code),
            station_name=station_name,
            mode_type=mode_type,
            interval="1day",
            year_start=str(Y1),
            month_start=str(M1),
            year_end=str(Y2),
            month_end=str(M2),
            value_col=data_label,
        )
        print(f"Parquetファイルを出力しました。 {parquet_path}")

    print(f"生成完了: {output_path}")
    return output_path


def show_water(
    parent,
    single_sheet_mode=False,
    on_open_other=None,
    on_close=None,
    on_return_home=None,
    debug_ui: bool = False,
    initial_codes=None,
):
    """Factory for launcher to create water_info window."""
    return _show_water(
        parent=parent,
        fetch_hourly=process_data_for_code,
        fetch_daily=process_period_date_display_for_code,
        empty_error_type=EmptyExcelWarning,
        single_sheet_mode=single_sheet_mode,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
        debug_ui=debug_ui,
        initial_codes=initial_codes,
    )
