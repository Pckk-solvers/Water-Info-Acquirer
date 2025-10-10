"""
型定義とデータモデル

データ取得・Excel出力で使用するdataclassとProtocolを定義
"""

from dataclasses import dataclass
from typing import Literal, Optional, Tuple
from pathlib import Path


@dataclass
class DataRequest:
    """データ取得リクエスト"""
    code: str
    start_year: int
    start_month: int
    end_year: int
    end_month: int
    mode: Literal["S", "R", "U"]  # 水位・流量・雨量
    granularity: Literal["hour", "day"]


@dataclass
class StationInfo:
    """観測所情報"""
    code: str
    name: str
    raw_name: str


@dataclass
class ChartConfig:
    """チャート設定"""
    title: str
    y_axis_label: str
    x_axis_format: str
    size: Tuple[int, int] = (720, 300)


@dataclass
class ExcelOptions:
    """Excel出力オプション"""
    single_sheet: bool = False
    include_summary: bool = True
    chart_config: Optional[ChartConfig] = None


@dataclass
class ModeConfig:
    """モード設定"""
    label: str
    unit: str
    file_suffix: str
    hour_config: 'UrlConfig'
    day_config: 'UrlConfig'


@dataclass
class UrlConfig:
    """URL設定"""
    num: str
    base_url: str
    encoding: str = "euc_jp"


# 時系列データの標準化されたDataFrame構造
# columns = ["datetime", "value", "display_dt", "sheet_year"]
# 
# datetime: 実測定時刻（0:00-23:00）
# display_dt: 表示用時刻（1:00-0:00、+1時間シフト）
# value: 測定値（数値またはNaN）
# sheet_year: シート分割用年（実測定時刻ベース）

TIMESERIES_COLUMNS = ["datetime", "value", "display_dt", "sheet_year"]