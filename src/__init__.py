"""
Water Info Acquirer - 水文データ取得ツール

水文水質データベースから水位・流量・雨量データを指定期間ごとに取得するツール
"""

__version__ = "0.1.1"
__author__ = "Water Info Acquirer Team"
__description__ = "水文水質データベースからデータから水位・流量・雨量データを指定期間ごとに取得する。"

# パッケージレベルでの主要なAPIをエクスポート
from .wia.api import execute_data_acquisition
from .wia.types import DataRequest, StationInfo, ExcelOptions
from .wia.errors import (
    WaterInfoAcquirerError,
    EmptyDataError,
    NetworkError,
    ParseError
)

__all__ = [
    "execute_data_acquisition",
    "DataRequest",
    "StationInfo", 
    "ExcelOptions",
    "WaterInfoAcquirerError",
    "EmptyDataError",
    "NetworkError",
    "ParseError",
]