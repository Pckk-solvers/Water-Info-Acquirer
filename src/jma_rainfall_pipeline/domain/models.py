"""
ビジネスロジック層のドメインモデル

このモジュールは、気象データ取得のビジネスロジックで使用するドメインモデルを定義します。
責務: ビジネスエンティティの表現とビジネスルールの適用
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from enum import Enum


class ObservationType(Enum):
    """観測所タイプ"""
    A1 = "a1"  # アメダス観測所
    S1 = "s1"  # 地域気象観測所


class DataInterval(Enum):
    """データ取得間隔"""
    MINUTE_10 = "10min"
    HOURLY = "1hour"
    DAILY = "1day"


@dataclass
class Station:
    """観測所情報"""
    prefecture_code: str
    block_number: str
    station_type: ObservationType
    station_name: Optional[str] = None
    station_id: Optional[str] = None

    def __post_init__(self):
        """初期化後の処理"""
        if isinstance(self.station_type, str):
            self.station_type = ObservationType(self.station_type)

    def to_dict(self) -> Dict[str, Any]:
        """観測所情報を辞書形式に変換"""
        return {
            "prefecture_code": self.prefecture_code,
            "block_number": self.block_number,
            "station_type": self.station_type.value,
            "station_name": self.station_name,
            "station_id": self.station_id,
        }


@dataclass
class WeatherDataRequest:
    """気象データ取得リクエスト"""
    stations: List[Station]
    start_date: datetime
    end_date: datetime
    interval: DataInterval = DataInterval.HOURLY
    include_metadata: bool = True
    output_format: str = "json"
    fields: Optional[List[str]] = None

    def __post_init__(self):
        """初期化後の処理"""
        if isinstance(self.interval, str):
            self.interval = DataInterval(self.interval)

        self.output_format = str(self.output_format).lower()
        if self.fields is not None:
            normalized_fields = [str(field).strip() for field in self.fields if str(field).strip()]
            self.fields = normalized_fields or None

        # 日付の検証
        if self.start_date >= self.end_date:
            raise ValueError("Start date must be before end date")

        # 最大期間の検証（1年以内）
        max_duration = timedelta(days=365)
        if self.end_date - self.start_date > max_duration:
            raise ValueError("Date range cannot exceed 1 year")

    def get_date_range_str(self) -> Dict[str, str]:
        """日付範囲をISO形式の文字列で取得"""
        return {
            "start": self.start_date.isoformat(),
            "end": self.end_date.isoformat(),
        }


@dataclass
class WeatherDataRecord:
    """気象データレコード"""
    timestamp: datetime
    station: Station
    temperature: Optional[float] = None
    precipitation: Optional[float] = None
    humidity: Optional[float] = None
    wind_speed: Optional[float] = None
    wind_direction: Optional[float] = None
    atmospheric_pressure: Optional[float] = None

    # 品質情報
    data_quality: str = "normal"
    missing_data_flags: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """レコードを辞書形式に変換"""
        base: Dict[str, Any] = {
            "timestamp": self.timestamp.isoformat(),
            "station": self.station.to_dict(),
            "temperature": self.temperature,
            "precipitation": self.precipitation,
            "humidity": self.humidity,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "atmospheric_pressure": self.atmospheric_pressure,
            "data_quality": self.data_quality,
            "missing_data_flags": list(self.missing_data_flags),
        }
        merged = dict(self.raw_data)
        merged.update({k: v for k, v in base.items() if k not in merged})
        return merged


@dataclass
class WeatherDataResult:
    """気象データ取得結果"""
    request: WeatherDataRequest
    records: List[WeatherDataRecord]
    total_count: int
    execution_time: Optional[float] = None
    errors: List[str] = field(default_factory=list)

    def is_success(self) -> bool:
        """処理が成功したかどうかを判定"""
        return len(self.errors) == 0 and len(self.records) > 0

    def get_summary(self) -> Dict[str, Any]:
        """結果のサマリーを取得"""
        return {
            "station_count": len(self.request.stations),
            "record_count": len(self.records),
            "date_range": {
                "start": self.request.start_date.isoformat(),
                "end": self.request.end_date.isoformat()
            },
            "interval": self.request.interval.value,
            "execution_time_seconds": self.execution_time,
            "success": self.is_success(),
            "error_count": len(self.errors)
        }

    def to_dict(self, include_records: bool = True) -> Dict[str, Any]:
        """結果全体を辞書形式に変換"""
        payload: Dict[str, Any] = {
            "summary": self.get_summary(),
            "errors": list(self.errors),
        }
        if include_records:
            payload["records"] = [record.to_dict() for record in self.records]
        return payload

    def to_dataframe(self) -> "pd.DataFrame":
        """結果をDataFrameに変換"""
        import pandas as pd  # 局所インポートで循環依存を回避

        if not self.records:
            return pd.DataFrame()
        return pd.DataFrame([record.to_dict() for record in self.records])

    def to_csv(self, fields: List[str]) -> str:
        """天候データを CSV 文字列に変換"""
        import csv
        import io

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for record in self.records:
            row = self._record_to_flat_dict(record)
            writer.writerow({field: row.get(field, "") for field in fields})
        return buffer.getvalue()

    @staticmethod
    def _record_to_flat_dict(record: WeatherDataRecord) -> Dict[str, Any]:
        station = record.station
        row: Dict[str, Any] = {
            "timestamp": record.timestamp.isoformat(),
            "prefecture_code": station.prefecture_code,
            "block_number": station.block_number,
            "station_type": station.station_type.value if station.station_type else "",
            "station_name": station.station_name or "",
            "station_id": station.station_id or "",
            "precipitation": record.precipitation,
            "temperature": record.temperature,
            "humidity": record.humidity,
            "wind_speed": record.wind_speed,
            "wind_direction": record.wind_direction,
            "atmospheric_pressure": record.atmospheric_pressure,
            "data_quality": record.data_quality,
            "missing_data_flags": "|".join(record.missing_data_flags) if record.missing_data_flags else "",
        }
        for key, value in record.raw_data.items():
            row.setdefault(key, value)
        return {key: ("" if value is None else value) for key, value in row.items()}


class BusinessRuleEngine:
    """ビジネスルールエンジン"""

    @staticmethod
    def validate_request(request: WeatherDataRequest) -> List[str]:
        """リクエストのビジネスルール検証"""
        errors = []

        # 観測所の数制限
        if len(request.stations) > 10:
            errors.append("Cannot request data for more than 10 stations at once")

        # データ量の見積もり（大まかなチェック）
        estimated_records = BusinessRuleEngine._estimate_record_count(request)
        if estimated_records > 50000:
            errors.append("Estimated data size exceeds limit (50,000 records)")

        return errors

    @staticmethod
    def _estimate_record_count(request: WeatherDataRequest) -> int:
        """データ件数の見積もり"""
        days = (request.end_date - request.start_date).days + 1

        # 間隔ごとの1日あたりのデータ件数
        records_per_day = {
            DataInterval.MINUTE_10: 144,  # 24時間 * 6（10分間隔）
            DataInterval.HOURLY: 24,
            DataInterval.DAILY: 1
        }

        base_records = days * records_per_day[request.interval]
        return base_records * len(request.stations)

    @staticmethod
    def optimize_request(request: WeatherDataRequest) -> WeatherDataRequest:
        """リクエストの最適化"""
        # 期間が長い場合はより粗い間隔を提案する処理をここに実装
        # 今回は単純にそのまま返す
        return request


class DomainEvent:
    """ドメインイベント"""

    def __init__(self, event_type: str, data: Dict[str, Any], timestamp: Optional[datetime] = None):
        self.event_type = event_type
        self.data = data
        self.timestamp = timestamp or datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_type": self.event_type,
            "data": self.data,
            "timestamp": self.timestamp.isoformat()
        }


@dataclass
class DataQualityMetrics:
    """データ品質指標"""
    total_records: int
    valid_records: int
    missing_data_percentage: float
    data_completeness_score: float

    def __post_init__(self):
        if self.total_records > 0:
            self.missing_data_percentage = ((self.total_records - self.valid_records) / self.total_records) * 100
            # データ完全性スコア（0-100）
            self.data_completeness_score = (self.valid_records / self.total_records) * 100
