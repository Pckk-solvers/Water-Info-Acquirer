"""ダム諸量専用の要求と項目定義。"""

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import calendar


@dataclass(frozen=True)
class DamMetric:
    key: str
    label: str
    unit: str


DAM_METRICS = (
    DamMetric("basin_rainfall", "流域平均雨量", "mm/h"),
    DamMetric("storage", "貯水量", "10^3 m3"),
    DamMetric("inflow", "流入量", "m3/s"),
    DamMetric("outflow", "放流量", "m3/s"),
    DamMetric("storage_rate", "貯水率", "%"),
)
QUALITY_LABELS = {
    "normal": "有効", "missing": "欠測", "unreceived": "未受信",
    "closed": "閉局", "unknown": "不明属性", "row_missing": "行欠落",
    "fetch_failed": "取得失敗",
}


@dataclass(frozen=True)
class DamRequest:
    start: date
    end: date
    output_dir: Path
    export_parquet: bool = False

    def __post_init__(self):
        if self.end < self.start:
            raise ValueError("取得期間が逆転しています")
        if self.start.year < 1900 or self.end.year > 9998:
            raise ValueError("取得年は1900〜9998の範囲で指定してください")


def iter_dam_windows(start: date, end: date):
    """元日付の両端を含み、同月かつ31日以内の窓を返す。"""
    if end < start:
        raise ValueError("取得期間が逆転しています")
    current = start
    while current <= end:
        last = date(current.year, current.month, calendar.monthrange(current.year, current.month)[1])
        stop = min(last, end)
        yield current, stop
        current = stop + timedelta(days=1)
