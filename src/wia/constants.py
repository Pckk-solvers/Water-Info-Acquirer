"""
定数・設定管理モジュール

モード設定、URL設定、Excel設定などの定数を管理
"""

from typing import Dict, Any

# モード設定のマッピングテーブル
MODE_CONFIG: Dict[str, Dict[str, Dict[str, str]]] = {
    "S": {  # 水位
        "hour": {"num": "2", "base_url": "DspWaterData.exe", "unit": "m"},
        "day": {"num": "3", "base_url": "DspWaterData.exe", "unit": "m"}
    },
    "R": {  # 流量
        "hour": {"num": "6", "base_url": "DspWaterData.exe", "unit": "m^3/s"},
        "day": {"num": "7", "base_url": "DspWaterData.exe", "unit": "m^3/s"}
    },
    "U": {  # 雨量
        "hour": {"num": "2", "base_url": "DspRainData.exe", "unit": "mm/h"},
        "day": {"num": "3", "base_url": "DspRainData.exe", "unit": "mm/h"}
    }
}

# モードラベル
MODE_LABELS: Dict[str, str] = {
    "S": "水位",
    "R": "流量", 
    "U": "雨量"
}

# モード単位
MODE_UNITS: Dict[str, str] = {
    "S": "m",
    "R": "m^3/s",
    "U": "mm/h"
}

# ファイル接尾辞
MODE_FILE_SUFFIXES: Dict[str, str] = {
    "S": "WH",
    "R": "QH", 
    "U": "RH"
}

# URL設定
BASE_URL: str = "http://www1.river.go.jp/cgi-bin/"
ENCODING: str = "euc_jp"

# Excel設定
DEFAULT_CHART_SIZE: tuple[int, int] = (720, 300)
COLUMN_WIDTHS: Dict[str, int] = {
    "datetime": 20,
    "value": 12,
    "summary": 15
}