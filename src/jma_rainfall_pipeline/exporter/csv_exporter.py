
# jma_rainfall_pipeline/exporter/csv_exporter.py
import json
from pathlib import Path
import pandas as pd
from datetime import date, datetime
from typing import Optional, List, Dict, Any

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
        'datetime', 'date', 'time', 'hour',
        'pressure_ground', 'pressure_sea', 'precipitation', 'precipitation_total',
        'temperature', 'dew_point', 'vapor_pressure', 'humidity',
        'wind_speed', 'wind_direction', 'sunshine_hours', 'solar_radiation',
        'snow_fall', 'snow_depth', 'weather', 'cloud_cover', 'visibility'
    ],
    'daily': [
        'date', 'precipitation_total', 'precipitation_max_1h', 'precipitation_max_10m', 'temperature_avg', 'temperature_max', 'temperature_min',
        'humidity_avg', 'wind_speed_avg', 'sunshine_hours', 'snow_depth'
    ],
    '10min': [
        'datetime', 'date', 'time', 'hour', 'minute',
        'pressure_ground', 'pressure_sea', 'precipitation', 'temperature', 'humidity',
        'wind_speed', 'wind_direction', 'wind_speed_max', 'wind_direction_max',
        'sunshine_minutes', 'solar_radiation'
    ]
}


def _normalize_time_column(df: pd.DataFrame) -> Optional[pd.Series]:
    """日時情報を正規化します。

    23:59:59.999999 のような時刻を翌日の 00:00:00 に変換する特別な処理を含みます。
    """
    try:
        # 日次データかどうかをチェック（'date'カラムはあるが'time'や'hour'カラムはない場合）
        is_daily = "date" in df.columns and "time" not in df.columns and "hour" not in df.columns

        if "datetime" in df.columns:
            # 直接datetimeカラムを処理
            normalized = pd.to_datetime(df["datetime"], errors="coerce")
            if is_daily:
                return normalized.dt.strftime("%Y/%m/%d")

            # 深夜0時近くの値をチェック
            near_midnight = (normalized.dt.hour == 23) & (normalized.dt.minute == 59)
            normalized[near_midnight] = normalized[near_midnight] + pd.Timedelta(minutes=1)
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        elif "date" in df.columns and "time" in df.columns:
            # 時刻が別カラムの場合の処理
            time_col = df["time"].astype(str).str.strip()

            # 23:59:59.999999 のようなパターンをチェック
            is_midnight = time_col.str.contains(r'23:59:59\.?\d*$', regex=True)

            # 日付シリーズを作成し、深夜の場合は1日加算
            date_series = pd.to_datetime(df["date"].astype(str).str.strip(), errors="coerce")
            date_series[is_midnight] = date_series[is_midnight] + pd.Timedelta(days=1)

            # 時刻をフォーマットし、23:59:59.xxx を 00:00:00 に置換
            time_series = time_col.replace(r'23:59:59\.?\d*$', '00:00:00', regex=True)

            # 日付と時刻を結合
            normalized = pd.to_datetime(
                date_series.dt.strftime("%Y-%m-%d") + " " + time_series,
                format='%Y-%m-%d %H:%M:%S',
                errors='coerce'
            )
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        elif "date" in df.columns and "hour" in df.columns:
            # 時間が別カラムの時間データを処理
            date_part = pd.to_datetime(df["date"], errors="coerce")
            hour_part = pd.to_numeric(df["hour"], errors="coerce").fillna(0)

            # 24時近くの時間（例：23.999）を処理
            is_midnight = (hour_part >= 23.99) & (hour_part <= 24.0)
            date_part[is_midnight] = date_part[is_midnight] + pd.Timedelta(days=1)
            hour_part[is_midnight] = 0

            # 浮動小数点の誤差を防ぐために分単位で丸める
            minutes = ((hour_part - hour_part.astype(int)) * 60).round().astype(int)
            hours = hour_part.astype(int)

            normalized = date_part + pd.to_timedelta(hours, unit='h') + pd.to_timedelta(minutes, unit='m')
            return normalized.dt.strftime("%Y/%m/%d %H:%M")

        elif "date" in df.columns:
            # 日付のみのデータを処理
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
            # 適切なExcel形式で保存するためにdatetimeオブジェクトとして処理
            if 'datetime' in df.columns:
                dt_series = pd.to_datetime(df['datetime'], errors='coerce')
            elif 'date' in df.columns and 'time' in df.columns:
                dt_series = pd.to_datetime(df['date'] + ' ' + df['time'], errors='coerce')
            elif 'date' in df.columns and 'hour' in df.columns:
                dt_series = pd.to_datetime(df['date'], errors='coerce') + \
                           pd.to_timedelta(df['hour'].fillna(0), unit='h')
            else:
                dt_series = pd.to_datetime(df['date'], errors='coerce')

            precipitation_df["日時"] = dt_series

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
                    # 日時データを文字列に変換（表示形式を統一）
                    if any(not pd.isna(t) and hasattr(t, 'hour') and (t.hour > 0 or t.minute > 0)
                           for t in precipitation_df["日時"] if pd.notna(t)):
                        # 時刻を含む場合
                        precipitation_df["日時"] = precipitation_df["日時"].dt.strftime('%Y/%m/%d %H:%M')
                        col_width = 16
                    else:
                        # 日付のみの場合
                        precipitation_df["日時"] = precipitation_df["日時"].dt.strftime('%Y/%m/%d')
                        col_width = 12

                    # データを再度書き込み
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
    output_dir: Path = None,
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

    if columns is None:
        columns = [col for col in OUTPUT_COLUMNS.get(interval, []) if col in df.columns]

    valid_columns = [col for col in columns if col in df.columns]
    if not valid_columns:
        valid_columns = df.columns.tolist()

    export_df = df[valid_columns]

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
