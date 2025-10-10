"""
Water Info Acquirer (WIA) Package

水文データ取得・整理支援ツールのリファクタリング版パッケージ
"""

__version__ = "2.0.0"
__author__ = "Water Info Acquirer Team"

# パッケージレベルでの主要APIのエクスポート
from .data_source import fetch_station_info, fetch_timeseries_data, generate_url
from .excel_writer import write_timeseries_excel
from .api import execute_data_acquisition, execute_single_station
from .errors import WaterInfoAcquirerError, EmptyDataError, NetworkError, ParseError
from .types import DataRequest, StationInfo, ExcelOptions
from .logging_config import setup_logging, get_logger
from .exception_handler import ExceptionHandler, create_gui_exception_handler, create_cli_exception_handler

__all__ = [
    "fetch_station_info",
    "fetch_timeseries_data", 
    "generate_url",
    "write_timeseries_excel",
    "execute_data_acquisition",
    "execute_single_station",
    "WaterInfoAcquirerError",
    "EmptyDataError",
    "NetworkError", 
    "ParseError",
    "DataRequest",
    "StationInfo",
    "ExcelOptions",
    "setup_logging",
    "get_logger",
    "ExceptionHandler",
    "create_gui_exception_handler",
    "create_cli_exception_handler",
]