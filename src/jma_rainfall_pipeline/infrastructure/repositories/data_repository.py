"""
インフラ層のデータアクセス実装

このモジュールは、データアクセス層の具体的な実装を提供します。
責務: JMAデータソースからのデータ取得と既存機能の統合
"""

import logging
from datetime import datetime, timedelta
from typing import Callable, List, Dict, Any, Optional

import pandas as pd

from .data_interfaces import IDataRepository, ICacheService, IDataParser, IDataExporter
from jma_rainfall_pipeline.domain.models import (
    Station,
    WeatherDataRequest,
    WeatherDataRecord,
    ObservationType,
    DataInterval,
)

logger = logging.getLogger(__name__)


class JMADataRepository(IDataRepository):
    """JMAデータリポジトリ実装"""

    def __init__(self):
        # 既存の機能との統合のため、既存のモジュールを活用
        self._jma_fetcher = None  # 後で依存性注入（既存のjma_codes_fetcher）
        self._parser = None       # 後で依存性注入（既存のparser）
        self._cache = None        # 後で依存性注入（CacheService経由）
        self._fetcher_factory: Optional[Callable[[DataInterval], Any]] = None

    # ------------------------------------------------------------------
    # 依存性注入用のセッター
    # ------------------------------------------------------------------
    def set_fetcher(self, fetcher) -> None:
        """JMAフェッチャーを設定"""
        self._jma_fetcher = fetcher

    def set_parser(self, parser: IDataParser) -> None:
        """データパーサーを設定"""
        self._parser = parser

    def set_cache_service(self, cache_service: ICacheService) -> None:
        """キャッシュサービスを設定"""
        self._cache = cache_service

    def set_fetcher_factory(self, factory: Callable[[DataInterval], Any]) -> None:
        """データ取得用フェッチャーを生成するファクトリを設定"""
        self._fetcher_factory = factory

    def get_all_stations(self) -> List[Station]:
        """Return all available observation stations."""
        try:
            return self._get_sample_stations()
        except Exception as exc:  # pragma: no cover - keep repository tolerant
            logger.error("Error getting all stations: %s", exc)
            return []

    def get_station_data(self, station: Station, request: WeatherDataRequest) -> List[WeatherDataRecord]:
        """
        観測所の気象データを取得

        実装では既存のfetcherとparserを活用し、
        新しいドメインモデルに変換する
        """
        try:
            # キャッシュチェック
            cache_key = f"weather_data_{station.prefecture_code}_{station.block_number}_{request.start_date.date()}_{request.end_date.date()}"
            if self._cache:
                cached_data = self._cache.get_cached_data(cache_key)
                if cached_data:
                    return self._convert_to_domain_model(cached_data, station)

            records = self._download_and_parse_station(station, request)

            # キャッシュに保存
            if self._cache:
                self._cache.set_cached_data(
                    cache_key,
                    [record.to_dict() for record in records],
                    ttl_seconds=3600,
                )

            return records

        except Exception as e:
            logger.error(f"Error getting station data for {station}: {e}")
            raise

    def get_stations_by_prefecture(self, prefecture_code: str) -> List[Station]:
        """都道府県別の観測所情報を取得"""
        try:
            # 既存のjma_codes_fetcherを活用
            # stations_data = self._jma_fetcher.fetch_station_codes(prefecture_code)
            # return [self._convert_station_data(station) for station in stations_data]

            # サンプル実装として仮のデータを返す
            all_stations = self._get_sample_stations()
            return [s for s in all_stations if s.prefecture_code == prefecture_code]

        except Exception as e:
            logger.error(f"Error getting stations for prefecture {prefecture_code}: {e}")
            return []

    def _convert_to_domain_model(self, data: Any, station: Station) -> List[WeatherDataRecord]:
        """データをドメインモデルに変換"""
        # 実装では、既存のparserからのデータ構造をドメインモデルに変換
        if not data:
            return []

        records: List[WeatherDataRecord] = []
        for entry in data:
            try:
                timestamp_value = entry.get("timestamp")
                timestamp = datetime.fromisoformat(timestamp_value) if isinstance(timestamp_value, str) else timestamp_value

                record = WeatherDataRecord(
                    timestamp=timestamp,
                    station=station,
                    temperature=entry.get("temperature"),
                    precipitation=entry.get("precipitation"),
                    humidity=entry.get("humidity"),
                    wind_speed=entry.get("wind_speed"),
                    wind_direction=entry.get("wind_direction"),
                    atmospheric_pressure=entry.get("atmospheric_pressure"),
                    data_quality=entry.get("data_quality", "normal"),
                    missing_data_flags=entry.get("missing_data_flags", []),
                )
                records.append(record)
            except Exception as exc:  # pragma: no cover - 個別レコードの変換失敗はログ出力のみ
                logger.warning("Failed to convert cached entry to domain model: %s", exc)
        return records

    def _convert_station_data(self, station_data: Dict[str, Any]) -> Station:
        """観測所データをドメインモデルに変換"""
        return Station(
            prefecture_code=station_data.get('prefecture_code', ''),
            block_number=station_data.get('block_number', ''),
            station_type=ObservationType.A1,  # デフォルト値
            station_name=station_data.get('station_name'),
            station_id=station_data.get('station_id')
        )

    def _download_and_parse_station(
        self,
        station: Station,
        request: WeatherDataRequest,
    ) -> List[WeatherDataRecord]:
        """気象庁サイトから観測所データを取得してドメインモデルに変換"""
        if not self._parser:
            raise RuntimeError("Parser service is not configured")
        if not self._fetcher_factory:
            raise RuntimeError("Fetcher factory is not configured")

        fetcher = self._fetcher_factory(request.interval)
        freq = self._resolve_frequency(request.interval)
        obs_type = station.station_type.value

        station_tuple = [(
            station.prefecture_code,
            station.block_number,
            obs_type,
        )]

        records: List[WeatherDataRecord] = []
        for (_, _), sample_dt, html in fetcher.schedule_fetch(
            station_tuple,
            request.start_date,
            request.end_date,
        ):
            try:
                df = self._parser(
                    html=html,
                    freq=freq,
                    sample_date=sample_dt.date(),
                    obs_type=obs_type,
                )
            except Exception as exc:  # pragma: no cover - パーサーエラーをログに記録
                logger.warning(
                    "Failed to parse html for station %s-%s: %s",
                    station.prefecture_code,
                    station.block_number,
                    exc,
                )
                continue

            if df is None or df.empty:
                continue

            for _, row in df.iterrows():
                record = self._row_to_record(row, station, sample_dt, request)
                if record:
                    records.append(record)

        if not records:
            # 取得できなかった場合でも空リストを返す（上位でリトライする可能性あり）
            logger.info(
                "No records fetched for station %s-%s between %s and %s",
                station.prefecture_code,
                station.block_number,
                request.start_date,
                request.end_date,
            )

        return records

    def _row_to_record(
        self,
        row: pd.Series,
        station: Station,
        sample_dt: datetime,
        request: WeatherDataRequest,
    ) -> Optional[WeatherDataRecord]:
        """DataFrameの1行をWeatherDataRecordに変換"""
        row_dict = row.to_dict()

        timestamp = self._extract_timestamp(row_dict, sample_dt, request.interval)
        if timestamp is None:
            return None

        row_dict.setdefault("prefecture_code", station.prefecture_code)
        row_dict.setdefault("block_number", station.block_number)
        row_dict.setdefault("station_type", station.station_type.value)

        return WeatherDataRecord(
            timestamp=timestamp,
            station=station,
            temperature=row_dict.get("temperature"),
            precipitation=row_dict.get("precipitation"),
            humidity=row_dict.get("humidity"),
            wind_speed=row_dict.get("wind_speed"),
            wind_direction=row_dict.get("wind_direction"),
            atmospheric_pressure=row_dict.get("pressure_ground"),
            data_quality=row_dict.get("data_quality", "normal"),
            missing_data_flags=row_dict.get("missing_data_flags", []),
            raw_data=row_dict,
        )

    def _extract_timestamp(
        self,
        row: Dict[str, Any],
        sample_dt: datetime,
        interval: DataInterval,
    ) -> Optional[datetime]:
        """行データからタイムスタンプを抽出"""
        ts = row.get("datetime")
        if ts is not None and not pd.isna(ts):
            return pd.to_datetime(ts, errors="coerce").to_pydatetime()

        date_value = row.get("date")
        time_value = row.get("time")
        hour_value = row.get("hour")

        if date_value is not None and not pd.isna(date_value):
            base_date = pd.to_datetime(date_value, errors="coerce")
            if pd.isna(base_date):
                base_date = pd.Timestamp(sample_dt.date())
        else:
            base_date = pd.Timestamp(sample_dt.date())

        if time_value and not pd.isna(time_value):
            timestamp = pd.to_datetime(f"{base_date.date()} {time_value}", errors="coerce")
            if pd.isna(timestamp):
                timestamp = base_date
        elif hour_value is not None and not pd.isna(hour_value):
            hour_float = float(hour_value)
            hours = int(hour_float)
            minutes = int(round((hour_float - hours) * 60))
            timestamp = base_date + pd.Timedelta(hours=hours, minutes=minutes)
        else:
            if interval == DataInterval.DAILY:
                timestamp = base_date
            elif interval == DataInterval.HOURLY:
                timestamp = base_date + pd.Timedelta(hours=sample_dt.hour)
            else:
                timestamp = pd.Timestamp(sample_dt)

        if timestamp is None or pd.isna(timestamp):
            return None
        return timestamp.to_pydatetime()

    def _resolve_frequency(self, interval: DataInterval) -> str:
        if interval == DataInterval.DAILY:
            return "daily"
        if interval == DataInterval.HOURLY:
            return "hourly"
        return "10min"

    def _resolve_timedelta(self, interval: DataInterval) -> timedelta:
        if interval == DataInterval.DAILY:
            return timedelta(days=1)
        if interval == DataInterval.HOURLY:
            return timedelta(hours=1)
        return timedelta(minutes=10)

    def _get_sample_stations(self) -> List[Station]:
        """サンプル観測所データを取得"""
        return [
            Station("11", "47401", ObservationType.S1, "さいたま"),
            Station("13", "44132", ObservationType.A1, "東京"),
            Station("27", "47891", ObservationType.S1, "大阪"),
            Station("14", "46201", ObservationType.A1, "横浜"),
            Station("23", "51101", ObservationType.S1, "名古屋"),
        ]


class CacheService(ICacheService):
    """キャッシュサービス実装"""

    def __init__(self):
        # 既存のcache_managerを活用
        self._cache_manager = None  # 後で依存性注入

    # ------------------------------------------------------------------
    # 依存性注入用のセッター
    # ------------------------------------------------------------------
    def set_cache_manager(self, cache_manager) -> None:
        """CacheManagerを設定"""
        self._cache_manager = cache_manager

    def get_cached_data(self, key: str) -> Optional[Any]:
        """キャッシュからデータを取得"""
        if not self._cache_manager or not self._cache_manager.enabled:
            return None

        return self._cache_manager.get_data(key)

    def set_cached_data(self, key: str, data: Any, ttl_seconds: Optional[int] = None) -> None:
        """キャッシュにデータを保存"""
        if not self._cache_manager or not self._cache_manager.enabled:
            return

        self._cache_manager.set_data(key, data, ttl_seconds)

    def invalidate_cache(self, key_pattern: str) -> None:
        """キャッシュを無効化"""
        if not self._cache_manager:
            return

        # パターンにマッチするキーを無効化
        self._cache_manager.invalidate_pattern(key_pattern)

    def clear_all_cache(self) -> None:
        """全キャッシュをクリア"""
        if not self._cache_manager:
            return

        self._cache_manager.clear_all()


class JMADataParser(IDataParser):
    """JMAデータパーサー実装"""

    def __init__(self):
        # 既存のparserモジュールを活用
        self._html_parser = None  # 後で依存性注入

    def parse_html_table(self, html_content: str, data_type: str) -> List[Dict[str, Any]]:
        """HTMLテーブルをパース"""
        # 既存のparserを活用した実装
        if self._html_parser:
            return self._html_parser.parse_html_table(html_content, data_type)
        return []

    def parse_station_list(self, html_content: str) -> List[Station]:
        """観測所リストをパース"""
        # 実装では既存のjma_codes_fetcherを活用
        return []

    def parse_weather_data(self, html_content: str, station: Station) -> List[WeatherDataRecord]:
        """気象データをパース"""
        # 実装では既存のparserを活用し、ドメインモデルに変換
        return []


class DataExporter(IDataExporter):
    """データエクスポーター実装"""

    def __init__(self):
        # 既存のcsv_exporterを活用
        self._csv_exporter = None  # 後で依存性注入

    def export_to_csv(self, records: List[WeatherDataRecord], output_path: str) -> str:
        """データをCSV形式でエクスポート"""
        # ドメインモデルをDataFrameに変換後、既存のエクスポーターを使用
        # df = self._convert_to_dataframe(records)
        # return self._csv_exporter.export_weather_data(df, output_path)
        return output_path  # 仮の実装

    def export_to_json(self, records: List[WeatherDataRecord], output_path: str) -> str:
        """データをJSON形式でエクスポート"""
        import json

        # ドメインモデルを辞書形式に変換
        data = [record.__dict__ for record in records]

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)

        return output_path

    def export_to_dataframe(self, records: List[WeatherDataRecord]) -> Any:
        """データをDataFrame形式でエクスポート"""
        import pandas as pd

        # ドメインモデルをDataFrameに変換
        data = []
        for record in records:
            row = {
                'timestamp': record.timestamp,
                'prefecture_code': record.station.prefecture_code,
                'block_number': record.station.block_number,
                'station_type': record.station.station_type.value,
                'station_name': record.station.station_name,
                'temperature': record.temperature,
                'precipitation': record.precipitation,
                'humidity': record.humidity,
                'wind_speed': record.wind_speed,
                'wind_direction': record.wind_direction,
                'atmospheric_pressure': record.atmospheric_pressure,
                'data_quality': record.data_quality
            }
            data.append(row)

        return pd.DataFrame(data)

    def _convert_to_dataframe(self, records: List[WeatherDataRecord]) -> Any:
        """ドメインモデルをDataFrameに変換"""
        return self.export_to_dataframe(records)
