"""Domain models for water_info."""

from __future__ import annotations

from dataclasses import dataclass

_MODE_TYPES = {"S", "R", "U"}


def _validate_year(year: str, label: str) -> None:
    if not year.isdigit() or len(year) != 4:
        raise ValueError(f"{label}は4桁の数字で指定してください: {year}")


def _parse_month(month: str, label: str) -> int:
    if not month.endswith("月"):
        raise ValueError(f"{label}は'1月'の形式で指定してください: {month}")
    raw = month[:-1]
    if not raw.isdigit():
        raise ValueError(f"{label}は'1月'の形式で指定してください: {month}")
    value = int(raw)
    if value < 1 or value > 12:
        raise ValueError(f"{label}は1月〜12月の範囲で指定してください: {month}")
    return value


@dataclass(frozen=True)
class Period:
    year_start: str
    year_end: str
    month_start: str
    month_end: str

    def __post_init__(self) -> None:
        _validate_year(self.year_start, "開始年")
        _validate_year(self.year_end, "終了年")
        start_month = _parse_month(self.month_start, "開始月")
        end_month = _parse_month(self.month_end, "終了月")
        y1 = int(self.year_start)
        y2 = int(self.year_end)
        if (y2, end_month) < (y1, start_month):
            raise ValueError("取得期間が逆転しています")


@dataclass(frozen=True)
class Options:
    use_daily: bool
    single_sheet: bool


@dataclass(frozen=True)
class WaterInfoRequest:
    period: Period
    mode_type: str
    options: Options

    def __post_init__(self) -> None:
        if self.mode_type not in _MODE_TYPES:
            raise ValueError(f"mode_typeは{sorted(_MODE_TYPES)}のいずれかを指定してください")
