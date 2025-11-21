"""
ビジネスロジック層のサービスクラス

このモジュールは、気象データ取得のビジネスロジックを実装します。
責務: ドメインルールの適用とビジネスプロセスの実行
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from ..models import (
    WeatherDataRequest, WeatherDataResult, WeatherDataRecord,
    Station, ObservationType, DataInterval, BusinessRuleEngine,
    DomainEvent, DataQualityMetrics
)

logger = logging.getLogger(__name__)


class WeatherDataService:
    """気象データ取得サービス"""

    def __init__(self):
        self._data_repository = None  # 後で依存性注入
        self._parser_service = None   # 後で依存性注入
        self._cache_service = None    # 後で依存性注入
        self._validator = None        # 後で依存性注入 (API層で定義されたバリデータなど)

    # ------------------------------------------------------------------
    # 依存性注入用のセッター
    # ------------------------------------------------------------------
    def set_data_repository(self, repository) -> None:
        """データリポジトリを設定"""
        self._data_repository = repository

    def set_parser_service(self, parser_service) -> None:
        """パーサーサービスを設定"""
        self._parser_service = parser_service

    def set_cache_service(self, cache_service) -> None:
        """キャッシュサービスを設定"""
        self._cache_service = cache_service

    def set_validator(self, validator) -> None:
        """リクエストバリデータを設定"""
        self._validator = validator

    def process_weather_data_request(self, request_data: Dict[str, Any]) -> WeatherDataResult:
        """
        気象データ取得リクエストを処理

        Args:
            request_data: リクエストデータ（API層から受け取ったデータ）

        Returns:
            WeatherDataResult: 処理結果
        """
        start_time = datetime.now()

        try:
            # リクエストオブジェクトの構築と検証
            sanitized_data = self._run_validator(request_data)
            request = self._build_request(sanitized_data)
            validation_errors = BusinessRuleEngine.validate_request(request)

            if validation_errors:
                return WeatherDataResult(
                    request=request,
                    records=[],
                    total_count=0,
                    errors=validation_errors,
                    execution_time=(datetime.now() - start_time).total_seconds()
                )

            # データ取得の実行
            records = self._fetch_weather_data(request)

            # データ品質の評価
            quality_metrics = self._evaluate_data_quality(records)

            # ドメインイベントの発行（ログや監査用）
            self._publish_domain_event("weather_data_retrieved", {
                "station_count": len(request.stations),
                "record_count": len(records),
                "date_range": request.get_date_range_str()
            })

            return WeatherDataResult(
                request=request,
                records=records,
                total_count=len(records),
                execution_time=(datetime.now() - start_time).total_seconds()
            )

        except Exception as e:
            logger.error(f"Error processing weather data request: {e}")
            return WeatherDataResult(
                request=request if 'request' in locals() else None,
                records=[],
                total_count=0,
                errors=[f"Internal processing error: {str(e)}"],
                execution_time=(datetime.now() - start_time).total_seconds()
            )

    def _build_request(self, request_data: Dict[str, Any]) -> WeatherDataRequest:
        """リクエストデータをWeatherDataRequestオブジェクトに変換"""
        required_keys = {'stations', 'start_date', 'end_date'}
        missing = [key for key in required_keys if key not in request_data]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")

        stations = []
        for idx, station_data in enumerate(request_data['stations']):
            if not isinstance(station_data, (list, tuple)) or len(station_data) < 3:
                raise ValueError(f"Station entry at index {idx} must be a tuple/list of (pref_code, block_no, obs_type)")
            station = Station(
                prefecture_code=str(station_data[0]),
                block_number=str(station_data[1]),
                station_type=station_data[2]
            )
            stations.append(station)

        interval = self._resolve_interval(request_data)

        return WeatherDataRequest(
            stations=stations,
            start_date=self._ensure_datetime(request_data['start_date'], 'start_date'),
            end_date=self._ensure_datetime(request_data['end_date'], 'end_date'),
            interval=interval,
            include_metadata=bool(request_data.get('include_metadata', True)),
            output_format=request_data.get('output_format', 'json'),
            fields=list(request_data.get('fields', [])) if request_data.get('fields') else None
        )

    def _fetch_weather_data(self, request: WeatherDataRequest) -> List[WeatherDataRecord]:
        """インフラ層からデータを取得"""
        all_records = []

        for station in request.stations:
            try:
                station_records = self._data_repository.get_station_data(station, request) if self._data_repository else None
                if not station_records:
                    station_records = self._generate_sample_records(station, request)
                all_records.extend(station_records)
            except Exception as e:
                logger.error(f"Error fetching data for station {station}: {e}")
                # エラーハンドリングはインフラ層に委譲

        return all_records

    def _generate_sample_records(self, station: Station, request: WeatherDataRequest) -> List[WeatherDataRecord]:
        """サンプルデータ生成（実装時はインフラ層から取得）"""
        records = []
        current_time = request.start_date

        while current_time <= request.end_date:
            record = WeatherDataRecord(
                timestamp=current_time,
                station=station,
                temperature=20.0 + (current_time.hour - 12) * 0.5,  # 簡易的な温度変化
                precipitation=max(0, (current_time.hour - 6) * 0.1 if 6 <= current_time.hour <= 18 else 0),
                humidity=60.0 + current_time.hour * 2,
                data_quality="normal"
            )
            records.append(record)

            # 時間間隔で時間を進める
            if request.interval == DataInterval.HOURLY:
                current_time += timedelta(hours=1)
            elif request.interval == DataInterval.MINUTE_10:
                current_time += timedelta(minutes=10)
            else:  # DAILY
                current_time += timedelta(days=1)

        return records

    def _evaluate_data_quality(self, records: List[WeatherDataRecord]) -> DataQualityMetrics:
        """データ品質を評価"""
        total_records = len(records)
        valid_records = sum(1 for record in records if record.data_quality == "normal")

        return DataQualityMetrics(
            total_records=total_records,
            valid_records=valid_records,
            missing_data_percentage=0.0,  # 実装時は実際の欠損データを計算
            data_completeness_score=(valid_records / total_records * 100) if total_records > 0 else 0.0
        )

    def _publish_domain_event(self, event_type: str, data: Dict[str, Any]):
        """ドメインイベントを発行"""
        event = DomainEvent(event_type, data)
        # 実装時はイベントバスやログにイベントを発行
        logger.info(f"Domain event: {event.to_dict()}")

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _run_validator(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """設定済みバリデータがあれば実行し、サニタイズ結果を返す"""
        if not self._validator:
            return request_data

        validation_result = self._validator.validate(request_data)
        if not getattr(validation_result, "is_valid", False):
            errors = getattr(validation_result, "errors", ["Unknown validation error"])
            raise ValueError(f"Request validation failed: {errors}")

        sanitized = getattr(validation_result, "sanitized_data", None)
        return sanitized or request_data

    def _ensure_datetime(self, value: Any, field_name: str) -> datetime:
        """入力値をdatetimeに変換"""
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except ValueError:
                try:
                    return datetime.strptime(value, '%Y-%m-%d')
                except ValueError as exc:
                    raise ValueError(f"Invalid {field_name} format: {value}") from exc
        raise ValueError(f"{field_name} must be a datetime or ISO formatted string")

    def _resolve_interval(self, request_data: Dict[str, Any]) -> DataInterval:
        """リクエストデータからデータ取得間隔を判定"""
        interval_value = request_data.get('interval')
        if isinstance(interval_value, DataInterval):
            return interval_value
        if isinstance(interval_value, str):
            try:
                return DataInterval(interval_value)
            except ValueError:
                mapping = {
                    '10min': DataInterval.MINUTE_10,
                    '1hour': DataInterval.HOURLY,
                    '1day': DataInterval.DAILY,
                    'daily': DataInterval.DAILY,
                    'hourly': DataInterval.HOURLY,
                }
                if interval_value in mapping:
                    return mapping[interval_value]

        minutes_value = request_data.get('interval_minutes')
        try:
            minutes_int = int(minutes_value) if minutes_value is not None else None
        except (TypeError, ValueError):
            minutes_int = None

        if minutes_int == 10:
            return DataInterval.MINUTE_10
        if minutes_int == 1440:
            return DataInterval.DAILY
        if minutes_int == 60:
            return DataInterval.HOURLY

        return DataInterval.HOURLY


class StationService:
    """観測所管理サービス"""

    def __init__(self):
        self._station_repository = None  # 後で依存性注入

    def get_available_stations(self, prefecture_code: Optional[str] = None) -> List[Station]:
        """利用可能な観測所を取得"""
        try:
            # インフラ層から観測所データを取得
            # stations_data = self._station_repository.get_all_stations()

            # 仮の実装としてサンプルデータを返す
            return self._get_sample_stations()

        except Exception as e:
            logger.error(f"Error getting available stations: {e}")
            return []

    def _get_sample_stations(self) -> List[Station]:
        """サンプル観測所データを取得"""
        return [
            Station("11", "47401", ObservationType.S1, "さいたま"),
            Station("13", "44132", ObservationType.A1, "東京"),
            Station("27", "47891", ObservationType.S1, "大阪"),
        ]

    def validate_station(self, station: Station) -> bool:
        """観測所の有効性を検証"""
        available_stations = self.get_available_stations()
        return any(
            s.prefecture_code == station.prefecture_code and
            s.block_number == station.block_number and
            s.station_type == station.station_type
            for s in available_stations
        )


class DataAggregationService:
    """データ集計サービス"""

    def __init__(self):
        self._export_service = None  # 後で依存性注入

    def aggregate_by_station(self, records: List[WeatherDataRecord]) -> Dict[str, List[WeatherDataRecord]]:
        """観測所ごとにデータを集計"""
        aggregated = {}
        for record in records:
            station_key = f"{record.station.prefecture_code}_{record.station.block_number}"
            if station_key not in aggregated:
                aggregated[station_key] = []
            aggregated[station_key].append(record)
        return aggregated

    def calculate_statistics(self, records: List[WeatherDataRecord]) -> Dict[str, Any]:
        """統計情報を計算"""
        if not records:
            return {}

        # 簡易的な統計計算（実装時はより詳細な統計を実装）
        temperatures = [r.temperature for r in records if r.temperature is not None]
        precipitations = [r.precipitation for r in records if r.precipitation is not None]

        return {
            "record_count": len(records),
            "temperature_stats": {
                "avg": sum(temperatures) / len(temperatures) if temperatures else None,
                "max": max(temperatures) if temperatures else None,
                "min": min(temperatures) if temperatures else None,
            },
            "precipitation_stats": {
                "total": sum(precipitations) if precipitations else 0,
                "avg": sum(precipitations) / len(precipitations) if precipitations else 0,
            }
        }
