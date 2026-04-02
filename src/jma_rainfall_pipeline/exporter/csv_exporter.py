
# jma_rainfall_pipeline/exporter/csv_exporter.py
import json
from pathlib import Path
import pandas as pd
from datetime import date, datetime
from typing import Optional, List, Dict, Any, cast

from jma_rainfall_pipeline.exporter.parquet_exporter import build_normalized_time_frame
from jma_rainfall_pipeline.logger.app_logger import get_logger
from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_prefecture_codes, fetch_station_codes
from jma_rainfall_pipeline.utils.config_loader import get_output_directories

logger = get_logger(__name__)
_PREF_MAP: Dict[str, str] = {}
_STATION_CACHE: Dict[str, List[Dict[str, str]]] = {}

SOURCE_NAME = "気象庁 気象統計情報"
SOURCE_URL = "https://www.data.jma.go.jp/"
PROCESSING_SUMMARY = "HTMLを解析し、列整形・欠損処理を行ったデータセットです。"


def _get_pref_map() -> Dict[str, str]:
    """Prefecture code/name cache."""
    if not _PREF_MAP:
        try:
            _PREF_MAP.update({code.zfill(2): name for code, name in fetch_prefecture_codes()})
        except Exception as exc:
            logger.warning(f"Failed to fetch prefecture codes: {exc}")
    return _PREF_MAP


def _get_station_records(prec_no: str) -> List[Dict[str, str]]:
    """Station list cache per prefecture."""
    key = str(prec_no).zfill(2)
    if key not in _STATION_CACHE:
        try:
            _STATION_CACHE[key] = fetch_station_codes(key)
        except Exception as exc:
            logger.warning(f"Failed to fetch station codes for {key}: {exc}")
            _STATION_CACHE[key] = []
    return _STATION_CACHE[key]


# data export columns
OUTPUT_COLUMNS = {
    'hourly': [
        'period_start_at', 'period_end_at', 'observed_at', 'datetime', 'date', 'time', 'hour',
        'pressure_ground', 'pressure_sea', 'precipitation', 'precipitation_total',
        'temperature', 'dew_point', 'vapor_pressure', 'humidity',
        'wind_speed', 'wind_direction', 'sunshine_hours', 'solar_radiation',
        'snow_fall', 'snow_depth', 'weather', 'cloud_cover', 'visibility'
    ],
    'daily': [
        'period_start_at', 'period_end_at', 'observed_at', 'date', 'precipitation_total', 'precipitation_max_1h', 'precipitation_max_10m', 'temperature_avg', 'temperature_max', 'temperature_min',
        'humidity_avg', 'wind_speed_avg', 'sunshine_hours', 'snow_depth'
    ],
    '10min': [
        'period_start_at', 'period_end_at', 'observed_at', 'datetime', 'date', 'time', 'hour', 'minute',
        'pressure_ground', 'pressure_sea', 'precipitation', 'temperature', 'humidity',
        'wind_speed', 'wind_direction', 'wind_speed_max', 'wind_direction_max',
        'sunshine_minutes', 'solar_radiation'
    ]
}


def _prepare_export_frame(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    work = build_normalized_time_frame(df, interval)
    if work.empty:
        return work

    if interval == "daily":
        work["date"] = pd.to_datetime(work["period_start_at"], errors="coerce").dt.strftime("%Y/%m/%d")
        return work

    observed = pd.to_datetime(work["observed_at"], errors="coerce")
    work["datetime"] = observed
    work["date"] = observed.dt.strftime("%Y/%m/%d")
    work["time"] = observed.dt.strftime("%H:%M")
    work["hour"] = observed.dt.hour
    if interval == "10min":
        work["minute"] = observed.dt.minute
    return work


def _normalize_time_column(df: pd.DataFrame) -> Optional[pd.Series]:
    """Excel 出力用の表示時刻列を返す。"""
    try:
        is_daily = "date" in df.columns and "time" not in df.columns and "hour" not in df.columns

        if "observed_at" in df.columns:
            normalized = pd.to_datetime(df["observed_at"], errors="coerce")
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        if "period_end_at" in df.columns:
            normalized = pd.to_datetime(df["period_end_at"], errors="coerce")
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        if "period_start_at" in df.columns:
            normalized = pd.to_datetime(df["period_start_at"], errors="coerce")
            if is_daily:
                return normalized.dt.strftime("%Y/%m/%d")
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        if "datetime" in df.columns:
            normalized = pd.to_datetime(df["datetime"], errors="coerce")
            if is_daily:
                return normalized.dt.strftime("%Y/%m/%d")
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        if "date" in df.columns:
            normalized = pd.to_datetime(df["date"], errors="coerce")
            return normalized.dt.strftime("%Y/%m/%d")

        return None

    except Exception as e:
        logger.warning(f"日時カラムの正規化中にエラーが発生しました: {e}")
        return None


def _export_precipitation_excel(
    df: pd.DataFrame,
    csv_path: Path,
    excel_output_dir: Optional[Path] = None,
    overview_info: Optional[Dict[str, Any]] = None,
) -> Optional[Path]:
    """降水量関連のカラムのみを含むExcelファイルを作成します。"""
    try:
        # カラム名のマッピング
        column_mapping = {
            "precipitation": "降水量",
            "precipitation_total": "合計降水量",
            "precipitation_max_1h": "1時間最大降水量",
            "precipitation_max_10m": "10分間最大降水量",
        }

        # 存在するカラムのみを抽出
        available_columns = {col: name for col, name in column_mapping.items() if col in df.columns}
        if not available_columns:
            return None

        precipitation_df = pd.DataFrame()

        # 日時カラムを正規化してExcel用のdatetimeオブジェクトとして保存
        normalized_time = _normalize_time_column(df)
        if normalized_time is not None:
            precipitation_df["日時"] = normalized_time

        # その他のカラムを追加
        for column, display_name in available_columns.items():
            precipitation_df[display_name] = df[column]

        if precipitation_df.empty:
            return None

        # Excelファイルの出力先を設定ファイルから取得
        if excel_output_dir is None:
            output_dirs = get_output_directories()
            excel_output_dir = Path(output_dirs['excel_dir'])
        else:
            excel_output_dir = Path(excel_output_dir)
        excel_output_dir.mkdir(parents=True, exist_ok=True)

        # CSVファイル名からExcelファイル名を生成
        excel_filename = csv_path.name.replace('.csv', '.xlsx')
        excel_path = excel_output_dir / excel_filename

        # XlsxWriterエンジンを使用してExcelライターを作成
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            # データフレームをExcelに書き込み
            precipitation_df.to_excel(writer, sheet_name='Sheet1', index=False)

            # ワークブックとワークシートオブジェクトを取得
            workbook = writer.book
            worksheet = writer.sheets['Sheet1']

            # 日時フォーマットを定義（表示形式を統一）
            datetime_format = workbook.add_format({
                'num_format': 'yyyy/mm/dd hh:mm',
                'align': 'left'
            })

            # 日時カラムの幅とフォーマットを設定
            if "日時" in precipitation_df.columns:
                # 日時カラムを文字列に変換してから書き込み
                if not precipitation_df["日時"].empty:
                    if any(" " in str(t) for t in precipitation_df["日時"] if pd.notna(t)):
                        col_width = 16
                    else:
                        col_width = 12

                    worksheet.write_column(1, 0, precipitation_df["日時"], datetime_format)
                    worksheet.set_column('A:A', col_width, datetime_format)
                else:
                    worksheet.set_column('A:A', 12, datetime_format)

            _write_source_sheet(workbook, overview_info)

        logger.info(f"Excelファイルを日時フォーマット付きで正常にエクスポートしました: {excel_path.resolve()}")
        return excel_path

    except Exception as e:
        logger.error(f"Excelエクスポート中にエラーが発生しました: {e}")
        return None


def _write_source_sheet(workbook: Any, overview_info: Optional[Dict[str, Any]]) -> None:
    """出典シートを末尾に追加する。"""
    if not overview_info:
        return

    sheet = workbook.add_worksheet("出典")
    exported_at = overview_info.get("exported_at")
    exported_str = exported_at.strftime("%Y-%m-%d %H:%M") if isinstance(exported_at, datetime) else str(exported_at)
    period = overview_info.get("period", {})
    station_code = overview_info.get("station_code")
    rows = [
        ("出典", SOURCE_NAME),
        ("URL", SOURCE_URL),
        ("取得日", exported_str),
        ("都道府県名", overview_info.get("prefecture_name")),
        ("観測所名", overview_info.get("station_name")),
        ("観測所コード", station_code),
        ("観測間隔", overview_info.get("interval")),
        ("取得期間(開始)", period.get("start")),
        ("取得期間(終了)", period.get("end")),
        ("取得項目", "降水量"),
        ("データ種別", "気象庁観測データ"),
        ("URLログ", "コンソール出力"),
        ("出力ファイル名", overview_info.get("output_file")),
        (
            "取得条件概要",
            f"{overview_info.get('prefecture_name')} {overview_info.get('station_name')} "
            f"({overview_info.get('interval')}) "
            f"{period.get('start')} - {period.get('end')}",
        ),
    ]

    for idx, (label, value) in enumerate(rows):
        sheet.write(idx, 0, label)
        sheet.write(idx, 1, value)

    sheet.set_column("A:A", 16)
    sheet.set_column("B:B", 80)


def _update_csv_metadata(csv_path: Path, entry: Dict[str, Any]) -> None:
    """CSVディレクトリ直下のmetadata.jsonを更新する。"""
    metadata_path = csv_path.parent / "metadata.json"
    data: Dict[str, Any]
    try:
        if metadata_path.exists():
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
        else:
            data = {}
    except json.JSONDecodeError:
        data = {}

    datasets = data.get("datasets", [])
    datasets = [item for item in datasets if item.get("file") != csv_path.name]
    datasets.append(entry)
    data["datasets"] = datasets
    data["updated_at"] = datetime.utcnow().isoformat()

    metadata_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def export_weather_data(
    df: pd.DataFrame,
    prec_no: str,
    block_no: str,
    interval: str,
    start_date: date,
    end_date: date,
    output_dir: Path | None = None,
    columns: Optional[List[str]] = None,
    export_csv: bool = True,
    export_excel: bool = True,
    excel_output_dir: Optional[Path] = None,
    request_urls: Optional[List[str]] = None,
) -> Path:
    """Export weather data to CSV and Excel."""
    logger.info(f"Starting data export for prefecture {prec_no}, station {block_no}, interval {interval}, period {start_date} to {end_date}")

    # 出力ディレクトリが指定されていない場合は設定ファイルから取得
    if output_dir is None:
        output_dirs = get_output_directories()
        output_dir = Path(output_dirs['csv_dir'])

    if export_csv:
        output_dir.mkdir(parents=True, exist_ok=True)

    pref_map = _get_pref_map()
    prec_key = str(prec_no).zfill(2)
    pref_name = pref_map.get(prec_key, f"pref_{prec_key}")

    station_records = _get_station_records(prec_key)
    block_key = str(block_no).strip()
    station_info = next((s for s in station_records if str(s.get("block_no")) == block_key), None)
    station_name = station_info.get("station", f"station_{block_key}") if station_info else f"station_{block_key}"
    request_url_list = sorted(set(request_urls or []))

    overview_info = {
        "prefecture_name": pref_name,
        "prefecture_code": prec_key,
        "station_name": station_name,
        "station_code": block_key,
        "interval": interval,
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat(),
        },
        "request_urls": request_url_list,
        "exported_at": datetime.utcnow(),
    }

    filename = f"{pref_name}_{station_name}_{interval}_{start_date.strftime('%Y%m%d')}-{end_date.strftime('%Y%m%d')}.csv"
    filepath = output_dir / filename
    overview_info["output_file"] = filename

    prepared_df = _prepare_export_frame(df, interval)
    if columns is None:
        columns = list(OUTPUT_COLUMNS.get(interval, prepared_df.columns.tolist()))
    valid_columns = [col for col in columns if col in prepared_df.columns]
    if not valid_columns:
        valid_columns = prepared_df.columns.tolist()
    export_df = cast(pd.DataFrame, prepared_df[valid_columns].copy())

    # CSV出力が有効な場合のみCSVファイルを作成
    if export_csv:
        export_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"CSVエクスポート完了: {interval}処理 - 都道府県コード: {prec_no}, 観測所番号: {block_no}, 出力ファイル: {filepath.resolve()}")
        metadata_entry = {
            "file": filepath.name,
            "prefecture": {"code": prec_key, "name": pref_name},
            "station": {"code": block_key, "name": station_name},
            "interval": interval,
            "period": {"start": start_date.isoformat(), "end": end_date.isoformat()},
            "source": {"name": SOURCE_NAME, "urls": request_url_list},
            "processing": {"summary": PROCESSING_SUMMARY, "processed": True},
            "exported_at": datetime.utcnow().isoformat(),
        }
        _update_csv_metadata(filepath, metadata_entry)
    else:
        logger.info("CSV export skipped as requested")

    # Excel出力が有効な場合のみExcelファイルを作成
    excel_path = None
    if export_excel:
        try:
            excel_path = _export_precipitation_excel(export_df, filepath, excel_output_dir, overview_info)
            if excel_path:
                logger.info(f"Excelエクスポート完了: {interval}処理 - 都道府県コード: {prec_no}, 観測所番号: {block_no}, 出力ファイル: {excel_path.resolve()}")
        except Exception as exc:
            logger.warning(f"Failed to export precipitation Excel file: {exc}")
    else:
        logger.info("Excel export skipped as requested")

    # 出力されたファイルのパスを返す（優先順位: CSV > Excel）
    if export_csv:
        return filepath
    elif excel_path:
        return excel_path
    else:
        # どちらも出力されない場合はCSVファイルのパスを返す（互換性のため）
        return filepath
