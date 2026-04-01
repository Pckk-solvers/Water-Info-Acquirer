from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

"""ドメインモデルと契約上の型定義。

このモジュールでは、Parquet 由来の時系列、基準線、スタイル設定、
および実行対象を表す軽量な dataclass を定義する。
"""

GraphType = Literal[
    "hyetograph",
    "hydrograph_discharge",
    "hydrograph_water_level",
    "annual_max_rainfall",
    "annual_max_discharge",
    "annual_max_water_level",
]

ReasonCode = Literal[
    "contract_error",
    "missing_timeseries",
    "insufficient_years",
    "threshold_not_found",
    "style_error",
    "render_error",
]

Source = Literal["jma", "water_info"]
Metric = Literal["rainfall", "water_level", "discharge"]
Interval = Literal["10min", "1hour", "1day"]


@dataclass(frozen=True, slots=True)
class ContractIssue:
    """契約違反や検証失敗の内容を表す。"""

    reason_code: ReasonCode
    reason_message: str
    source: str | None = None
    station_key: str | None = None
    graph_type: str | None = None
    row_index: int | None = None


@dataclass(frozen=True, slots=True)
class TimeseriesRecord:
    """保存済み Parquet の 1 レコードを表す。"""

    source: Source
    station_key: str
    station_name: str
    observed_at: datetime
    metric: Metric
    value: float | None
    unit: str
    interval: Interval
    quality: str

    def to_dict(self) -> dict[str, Any]:
        """外部出力やテスト用に辞書へ変換する。"""

        return {
            "source": self.source,
            "station_key": self.station_key,
            "station_name": self.station_name,
            "observed_at": self.observed_at.isoformat(),
            "metric": self.metric,
            "value": self.value,
            "unit": self.unit,
            "interval": self.interval,
            "quality": self.quality,
        }


@dataclass(frozen=True, slots=True)
class ThresholdRecord:
    """観測所・グラフ種別に紐づく基準線 1 件を表す。"""

    source: Source
    station_key: str
    graph_type: GraphType
    line_name: str
    value: float
    unit: str
    line_color: str | None = None
    line_style: str | None = None
    line_width: float | None = None
    label: str | None = None
    priority: int = 0
    enabled: bool = True
    note: str | None = None
    order_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        """基準線設定を辞書へ変換する。"""

        return {
            "source": self.source,
            "station_key": self.station_key,
            "graph_type": self.graph_type,
            "line_name": self.line_name,
            "value": self.value,
            "unit": self.unit,
            "line_color": self.line_color,
            "line_style": self.line_style,
            "line_width": self.line_width,
            "label": self.label,
            "priority": self.priority,
            "enabled": self.enabled,
            "note": self.note,
        }


@dataclass(slots=True)
class StyleConfig:
    """描画スタイルの正規化後表現。"""

    schema_version: str
    common: dict[str, Any]
    graph_styles: dict[GraphType, dict[str, Any]]
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """保存可能な形へ戻す。"""

        return {
            "schema_version": self.schema_version,
            "common": dict(self.common),
            "graph_styles": {key: dict(value) for key, value in self.graph_styles.items()},
        }


@dataclass(frozen=True, slots=True)
class GraphTarget:
    """バッチ実行やプレビューで扱う 1 つの描画対象。"""

    source: str
    station_key: str
    graph_type: GraphType
    base_date: date | None = None
    event_window_days: int | None = None

    @property
    def target_id(self) -> str:
        """画面表示やログ出力で使う一意の識別子を返す。"""

        base = self.base_date.isoformat() if self.base_date else "annual"
        if self.base_date and self.event_window_days in (3, 5):
            return f"{self.source}:{self.station_key}:{self.graph_type}:{base}:{self.event_window_days}day"
        return f"{self.source}:{self.station_key}:{self.graph_type}:{base}"


ThresholdLine = ThresholdRecord
