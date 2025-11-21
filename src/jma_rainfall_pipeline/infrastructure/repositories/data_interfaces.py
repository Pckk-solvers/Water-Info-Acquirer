"""
インフラ層のデータアクセスインターフェース

このモジュールは、データアクセス層のインターフェースを定義します。
責務: データソースからのデータ取得とキャッシュ管理
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime

from jma_rainfall_pipeline.domain.models import (
    Station,
    WeatherDataRequest,
    WeatherDataRecord,
)


class IDataRepository(ABC):
    """データリポジトリのインターフェース"""

    @abstractmethod
    def get_station_data(self, station: Station, request: WeatherDataRequest) -> List[WeatherDataRecord]:
        """観測所の気象データを取得"""
        pass

    @abstractmethod
    def get_all_stations(self) -> List[Station]:
        """全観測所情報を取得"""
        pass

    @abstractmethod
    def get_stations_by_prefecture(self, prefecture_code: str) -> List[Station]:
        """都道府県別の観測所情報を取得"""
        pass


class ICacheService(ABC):
    """キャッシュサービスのインターフェース"""

    @abstractmethod
    def get_cached_data(self, key: str) -> Optional[Any]:
        """キャッシュからデータを取得"""
        pass

    @abstractmethod
    def set_cached_data(self, key: str, data: Any, ttl_seconds: Optional[int] = None) -> None:
        """キャッシュにデータを保存"""
        pass

    @abstractmethod
    def invalidate_cache(self, key_pattern: str) -> None:
        """キャッシュを無効化"""
        pass

    @abstractmethod
    def clear_all_cache(self) -> None:
        """全キャッシュをクリア"""
        pass


class IDataParser(ABC):
    """データパーサーのインターフェース"""

    @abstractmethod
    def parse_html_table(self, html_content: str, data_type: str) -> List[Dict[str, Any]]:
        """HTMLテーブルをパースしてデータを抽出"""
        pass

    @abstractmethod
    def parse_station_list(self, html_content: str) -> List[Station]:
        """観測所リストをパース"""
        pass

    @abstractmethod
    def parse_weather_data(self, html_content: str, station: Station) -> List[WeatherDataRecord]:
        """気象データをパース"""
        pass


class IDataExporter(ABC):
    """データエクスポーターのインターフェース"""

    @abstractmethod
    def export_to_csv(self, records: List[WeatherDataRecord], output_path: str) -> str:
        """データをCSV形式でエクスポート"""
        pass

    @abstractmethod
    def export_to_json(self, records: List[WeatherDataRecord], output_path: str) -> str:
        """データをJSON形式でエクスポート"""
        pass

    @abstractmethod
    def export_to_dataframe(self, records: List[WeatherDataRecord]) -> Any:
        """データをDataFrame形式でエクスポート"""
        pass


class IConfigurationService(ABC):
    """設定管理サービスのインターフェース"""

    @abstractmethod
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """設定値を取得"""
        pass

    @abstractmethod
    def set_config_value(self, key: str, value: Any) -> None:
        """設定値を保存"""
        pass

    @abstractmethod
    def get_all_config(self) -> Dict[str, Any]:
        """全設定を取得"""
        pass
