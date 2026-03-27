from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import pandas as pd


RainfallSource = Literal["jma", "water_info"]
RainfallInterval = Literal["10min", "1hour", "1day"]
TimeseriesMetric = Literal["rainfall", "water_level", "discharge"]


@dataclass(slots=True, frozen=True)
class JMAStationInput:
    prefecture_code: str
    block_number: str
    obs_type: str = "a1"
    station_name: str = ""
    start_date: str = ""

    @property
    def station_key(self) -> str:
        return f"{self.prefecture_code}_{self.block_number}"


@dataclass(slots=True, frozen=True)
class WaterInfoStationInput:
    station_code: str
    station_name: str = ""

    @property
    def station_key(self) -> str:
        return self.station_code


@dataclass(slots=True, frozen=True)
class RainfallQuery:
    start_at: datetime
    end_at: datetime
    interval: RainfallInterval

    def __post_init__(self) -> None:
        if self.start_at > self.end_at:
            raise ValueError("start_at must be earlier than or equal to end_at")


@dataclass(slots=True)
class RainfallRecord:
    source: RainfallSource
    station_key: str
    station_name: str
    observed_at: datetime
    interval: RainfallInterval
    rainfall_mm: float | None
    quality: str = "normal"
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "station_key": self.station_key,
            "station_name": self.station_name,
            "observed_at": self.observed_at.isoformat(),
            "interval": self.interval,
            "rainfall_mm": self.rainfall_mm,
            "quality": self.quality,
            "raw": dict(self.raw),
        }


@dataclass(slots=True)
class UnifiedTimeseriesRecord:
    source: RainfallSource
    station_key: str
    station_name: str
    observed_at: datetime
    metric: TimeseriesMetric
    value: float | None
    unit: str
    interval: RainfallInterval
    quality: str = "normal"
    raw: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
            "raw": dict(self.raw),
        }


@dataclass(slots=True)
class RainfallDataset:
    records: list[RainfallRecord] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dataframe(self) -> "pd.DataFrame":
        import pandas as pd

        if not self.records:
            return pd.DataFrame()
        return pd.DataFrame([record.to_dict() for record in self.records])
