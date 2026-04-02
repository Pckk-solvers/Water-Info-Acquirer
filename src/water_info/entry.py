"""Unified entry points for water_info."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from .domain.models import Options, Period, WaterInfoRequest
from .service.flow_fetch import fetch_hourly_dataframe_for_code, fetch_daily_dataframe_for_code
from .service.flow_write import write_hourly_excel, write_daily_excel
from .infra.http_client import HEADERS, throttled_get
from .infra.url_builder import build_daily_base_url, build_hourly_base
from .ui.app import show_water as _show_water


class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass


@dataclass(frozen=True)
class WaterInfoOutputResult:
    code: str
    station_name: str
    excel_path: Path | None
    parquet_path: Path | None
    csv_records: tuple[dict[str, Any], ...] = ()


_UNIFIED_COLUMNS = (
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


def _records_to_unified_dataframe(records: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> pd.DataFrame:
    df = pd.DataFrame(records, columns=pd.Index(_UNIFIED_COLUMNS))
    if df.empty:
        df = pd.DataFrame(columns=pd.Index(_UNIFIED_COLUMNS))
    df["period_start_at"] = pd.to_datetime(cast(pd.Series, df["period_start_at"]), errors="coerce")
    df["period_end_at"] = pd.to_datetime(cast(pd.Series, df["period_end_at"]), errors="coerce")
    df["observed_at"] = pd.to_datetime(cast(pd.Series, df["observed_at"]), errors="coerce")
    return df


def _save_unified_records_parquet(records: list[dict[str, Any]], output_path: Path) -> Path:
    df = _records_to_unified_dataframe(records)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    return output_path


def save_unified_records_csv(
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    output_path: Path,
) -> Path:
    df = _records_to_unified_dataframe(records)
    if not df.empty:
        df = df.sort_values(["station_key", "period_start_at"], kind="stable").reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig", date_format="%Y-%m-%dT%H:%M:%S")
    return output_path


def _resolve_output_root(output_dir: str | Path | None = None) -> Path:
    if output_dir is None:
        base_dir = Path("outputs") / "water_info"
    else:
        base_dir = Path(output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def _resolve_output_path(file_name: str | Path, output_dir: str | Path | None = None) -> Path:
    return _resolve_output_root(output_dir) / Path(file_name).name


def _resolve_parquet_dir(output_dir: str | Path | None = None) -> Path:
    out_dir = _resolve_output_root(output_dir) / "parquet"
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
    output_dir: str | Path | None = None,
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
    return _resolve_parquet_dir(output_dir) / name


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
    output_dir: str | Path | None = None,
) -> Path:
    rows = _build_water_info_unified_records(
        df=df,
        code=code,
        station_name=station_name,
        mode_type=mode_type,
        interval=interval,
        value_col=value_col,
    )
    out_path = _build_water_info_parquet_path(
        code=code,
        mode_type=mode_type,
        interval=interval,
        year_start=year_start,
        month_start=month_start,
        year_end=year_end,
        month_end=month_end,
        output_dir=output_dir,
    )

    return _save_unified_records_parquet(rows, out_path)


def _build_water_info_unified_records(
    *,
    df: pd.DataFrame,
    code: str,
    station_name: str,
    mode_type: str,
    interval: str,
    value_col: str,
) -> list[dict[str, Any]]:
    metric, unit = _metric_and_unit(mode_type)
    station_key = str(code)
    rows: list[dict[str, Any]] = []
    instantaneous = mode_type in {"S", "R"}

    if interval == "1day":
        date_idx = (
            df.index
            if "datetime" not in df.columns
            else pd.to_datetime(cast(pd.Series, df["datetime"]), errors="coerce")
        )
        for idx, value_raw in zip(date_idx, df[value_col]):
            observed_at = pd.to_datetime(idx, errors="coerce")
            if not isinstance(observed_at, pd.Timestamp):
                continue
            value = pd.to_numeric(value_raw, errors="coerce")
            value_float = float(value) if isinstance(value, (int, float)) and not pd.isna(value) else None
            period_start_at = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
            period_end_at = period_start_at + pd.Timedelta(days=1)
            stored_period_start = None if instantaneous else period_start_at.to_pydatetime()
            stored_period_end = None if instantaneous else period_end_at.to_pydatetime()
            stored_observed = (
                period_start_at.to_pydatetime() if instantaneous else period_end_at.to_pydatetime()
            )
            rows.append(
                {
                    "source": "water_info",
                    "station_key": station_key,
                    "station_name": station_name or "",
                    "period_start_at": stored_period_start,
                    "period_end_at": stored_period_end,
                    "observed_at": stored_observed,
                    "metric": metric,
                    "value": value_float,
                    "unit": unit,
                    "interval": interval,
                    "quality": "normal" if value_float is not None else "missing",
                }
            )
        return rows

    datetime_col = df.get("datetime")
    datetimes = pd.to_datetime(cast(pd.Series, datetime_col), errors="coerce") if datetime_col is not None else None
    if datetimes is None:
        datetimes = pd.Series(dtype="datetime64[ns]")
    for timestamp, value_raw in zip(datetimes, df[value_col]):
        if not isinstance(timestamp, pd.Timestamp):
            continue
        value = pd.to_numeric(value_raw, errors="coerce")
        value_float = float(value) if isinstance(value, (int, float)) and not pd.isna(value) else None
        period_end_at = timestamp
        period_start_at = timestamp - pd.Timedelta(hours=1)
        stored_period_start = None if instantaneous else period_start_at.to_pydatetime()
        stored_period_end = None if instantaneous else period_end_at.to_pydatetime()
        stored_observed = timestamp.to_pydatetime() if instantaneous else period_end_at.to_pydatetime()
        rows.append(
            {
                "source": "water_info",
                "station_key": station_key,
                "station_name": station_name or "",
                "period_start_at": stored_period_start,
                "period_end_at": stored_period_end,
                "observed_at": stored_observed,
                "metric": metric,
                "value": value_float,
                "unit": unit,
                "interval": interval,
                "quality": "normal" if value_float is not None else "missing",
            }
        )
    return rows


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
    request = _build_legacy_request(
        year_start=str(Y1),
        year_end=str(Y2),
        month_start=str(M1),
        month_end=str(M2),
        mode_type=str(mode_type),
        single_sheet=single_sheet,
        export_parquet=export_parquet,
        use_daily=False,
    )
    result = run_cli_request_for_code(
        code=str(code),
        request=request,
        output_dir=None,
        progress_callback=progress_callback,
    )
    if result.parquet_path is not None:
        print(f"Parquetファイルを出力しました。 {result.parquet_path}")
    print(f"Excelファイルの作成が完了しました。 {result.excel_path}")
    return result.excel_path


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
    request = _build_legacy_request(
        year_start=str(Y1),
        year_end=str(Y2),
        month_start=str(M1),
        month_end=str(M2),
        mode_type=str(mode_type),
        single_sheet=single_sheet,
        export_parquet=export_parquet,
        use_daily=True,
    )
    result = run_cli_request_for_code(
        code=str(code),
        request=request,
        output_dir=None,
        progress_callback=progress_callback,
    )
    if result.parquet_path is not None:
        print(f"Parquetファイルを出力しました。 {result.parquet_path}")
    print(f"生成完了: {result.excel_path}")
    return result.excel_path


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


def run_cli_request_for_code(
    *,
    code: str,
    request: WaterInfoRequest,
    output_dir: str | Path | None = None,
    progress_callback=None,
) -> WaterInfoOutputResult:
    if request.options.use_daily:
        return _run_daily_request_for_code(
            code=code,
            request=request,
            output_dir=output_dir,
            progress_callback=progress_callback,
        )
    return _run_hourly_request_for_code(
        code=code,
        request=request,
        output_dir=output_dir,
        progress_callback=progress_callback,
    )


def _run_hourly_request_for_code(
    *,
    code: str,
    request: WaterInfoRequest,
    output_dir: str | Path | None,
    progress_callback=None,
) -> WaterInfoOutputResult:
    period = request.period
    options = request.options
    df, file_name, value_col = fetch_hourly_dataframe_for_code(
        code=code,
        year_start=period.year_start,
        year_end=period.year_end,
        month_start=period.month_start,
        month_end=period.month_end,
        mode_type=request.mode_type,
        throttled_get=throttled_get,
        headers=HEADERS,
        progress_callback=progress_callback,
    )
    if df is None or not value_col:
        raise ValueError(f"観測所コード {code}：時刻データを取得できませんでした")

    hourly_df = cast(pd.DataFrame, df)
    if hourly_df.empty or cast(pd.Series, hourly_df[value_col]).dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    _, mode_str = build_hourly_base(request.mode_type)
    station_name = file_name.name.split("_")[1] if file_name else ""
    output_path = _resolve_output_path(file_name if file_name else f"{code}.xlsx", output_dir)
    source_info = {
        "source_name": "国土交通省 水文水質データベース",
        "source_url": f"http://www1.river.go.jp/cgi-bin/Dsp{mode_str}Data.exe",
        "station_name": station_name,
        "station_code": code,
        "period": f"{period.year_start}/{period.month_start}-{period.year_end}/{period.month_end}",
        "period_start": f"{period.year_start}/{period.month_start}",
        "period_end": f"{period.year_end}/{period.month_end}",
        "item": source_info_item_label(request.mode_type),
        "data_kind": "時刻",
        "url_log": "コンソール出力",
        "output_file": output_path.name,
        "summary": (
            f"{station_name}({code}) {period.year_start}/{period.month_start}-"
            f"{period.year_end}/{period.month_end} {source_info_item_label(request.mode_type)}"
        ),
    }
    csv_records = tuple(
        _build_water_info_unified_records(
            df=hourly_df,
            code=str(code),
            station_name=station_name,
            mode_type=request.mode_type,
            interval="1hour",
            value_col=value_col,
        )
    )
    excel_path: Path | None = None
    if options.export_excel:
        write_hourly_excel(
            df=hourly_df,
            file_name=output_path,
            value_col=value_col,
            mode_type=request.mode_type,
            single_sheet=options.single_sheet,
            source_info=source_info,
            empty_error_type=EmptyExcelWarning,
        )
        excel_path = output_path
    parquet_path = None
    if options.export_parquet:
        parquet_path = _export_water_info_parquet(
            df=hourly_df,
            code=str(code),
            station_name=station_name,
            mode_type=request.mode_type,
            interval="1hour",
            year_start=str(period.year_start),
            month_start=str(period.month_start),
            year_end=str(period.year_end),
            month_end=str(period.month_end),
            value_col=value_col,
            output_dir=output_dir,
        )
    return WaterInfoOutputResult(
        code=str(code),
        station_name=station_name,
        excel_path=excel_path,
        parquet_path=parquet_path,
        csv_records=csv_records,
    )


def _run_daily_request_for_code(
    *,
    code: str,
    request: WaterInfoRequest,
    output_dir: str | Path | None,
    progress_callback=None,
) -> WaterInfoOutputResult:
    period = request.period
    options = request.options
    df, file_name, data_label, chart_title = fetch_daily_dataframe_for_code(
        code=code,
        year_start=period.year_start,
        year_end=period.year_end,
        month_start=period.month_start,
        month_end=period.month_end,
        mode_type=request.mode_type,
        throttled_get=throttled_get,
        headers=HEADERS,
        progress_callback=progress_callback,
    )
    if df is None or not data_label or not chart_title:
        raise ValueError(f"観測所コード {code}：日データを取得できませんでした")

    daily_df = cast(pd.DataFrame, df)
    if daily_df.empty or cast(pd.Series, daily_df[data_label]).dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    station_name = file_name.name.split("_")[1] if file_name else ""
    output_path = _resolve_output_path(file_name if file_name else f"{code}.xlsx", output_dir)
    source_info = {
        "source_name": "国土交通省 水文水質データベース",
        "source_url": build_daily_base_url(request.mode_type),
        "station_name": station_name,
        "station_code": code,
        "period": f"{period.year_start}/{period.month_start}-{period.year_end}/{period.month_end}",
        "period_start": f"{period.year_start}/{period.month_start}",
        "period_end": f"{period.year_end}/{period.month_end}",
        "item": source_info_item_label(request.mode_type),
        "data_kind": "日",
        "url_log": "コンソール出力",
        "output_file": output_path.name,
        "summary": (
            f"{station_name}({code}) {period.year_start}/{period.month_start}-"
            f"{period.year_end}/{period.month_end} {source_info_item_label(request.mode_type)}"
        ),
    }
    csv_records = tuple(
        _build_water_info_unified_records(
            df=daily_df,
            code=str(code),
            station_name=station_name,
            mode_type=request.mode_type,
            interval="1day",
            value_col=data_label,
        )
    )
    excel_path: Path | None = None
    if options.export_excel:
        write_daily_excel(
            df=daily_df,
            file_name=output_path,
            data_label=data_label,
            chart_title=chart_title,
            single_sheet=options.single_sheet,
            source_info=source_info,
        )
        excel_path = output_path
    parquet_path = None
    if options.export_parquet:
        parquet_path = _export_water_info_parquet(
            df=daily_df,
            code=str(code),
            station_name=station_name,
            mode_type=request.mode_type,
            interval="1day",
            year_start=str(period.year_start),
            month_start=str(period.month_start),
            year_end=str(period.year_end),
            month_end=str(period.month_end),
            value_col=data_label,
            output_dir=output_dir,
        )
    return WaterInfoOutputResult(
        code=str(code),
        station_name=station_name,
        excel_path=excel_path,
        parquet_path=parquet_path,
        csv_records=csv_records,
    )


def _build_legacy_request(
    *,
    year_start: str,
    year_end: str,
    month_start: str,
    month_end: str,
    mode_type: str,
    single_sheet: bool,
    export_parquet: bool,
    use_daily: bool,
) -> WaterInfoRequest:
    return WaterInfoRequest(
        period=Period(
            year_start=str(year_start),
            year_end=str(year_end),
            month_start=str(month_start),
            month_end=str(month_end),
        ),
        mode_type=str(mode_type),
        options=Options(
            use_daily=use_daily,
            single_sheet=single_sheet,
            export_excel=True,
            export_parquet=export_parquet,
            export_csv=False,
        ),
    )
