from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .rainfall import RainfallRunInput


def normalize_collection_order(collection_order: str) -> str:
    token = str(collection_order or "").strip().lower().replace("-", "_").replace(" ", "_")
    if token in {"station_year", "stationyear"}:
        return "station_year"
    if token in {"year_station", "yearstation"}:
        return "year_station"
    raise ValueError(
        "collection_order must be one of: station_year, year_station"
    )


def resolve_target_years_for_analyze(config: "RainfallRunInput") -> list[int]:
    years = config.years if config.years else ([config.year] if config.year else [])
    years = [int(year) for year in years]
    years = list(dict.fromkeys(years))
    if years:
        if config.start_at is not None or config.end_at is not None:
            raise ValueError("Specify either year/years or start_at/end_at, not both")
        start_year = min(years)
        end_year = max(years)
        if start_year < 1900 or end_year > 2100:
            raise ValueError(f"Unsupported year range: {start_year}-{end_year}")
        return years

    if config.start_at is not None and config.end_at is not None:
        if config.start_at > config.end_at:
            raise ValueError("start_at must be earlier than or equal to end_at")
        start_year = config.start_at.year
        end_year = config.end_at.year
        if start_year < 1900 or end_year > 2100:
            raise ValueError(f"Unsupported year range: {start_year}-{end_year}")
        return list(range(start_year, end_year + 1))

    if config.start_at is None and config.end_at is None:
        raise ValueError("start_at/end_at or year(s) is required")
    raise ValueError("Both start_at and end_at are required when year is not specified")


def format_target_years_normalization_log(config: "RainfallRunInput", target_years: list[int]) -> str:
    normalized_text = format_years_list_text(target_years)
    normalized_period_text = format_years_period_text(target_years)

    if config.years or config.year is not None:
        raw_years = config.years if config.years else ([config.year] if config.year is not None else [])
        raw_text = format_years_list_text([int(year) for year in raw_years])
        return (
            "[collect][period][global] 入力=年指定。"
            f"指定年={raw_text} -> 正規化後対象年={normalized_text} / 取得期間={normalized_period_text} "
            "理由=取得処理は年単位ジョブで実行するため（重複年は除外）"
        )

    start_text = config.start_at.strftime("%Y-%m-%d %H:%M:%S") if config.start_at else "(未指定)"
    end_text = config.end_at.strftime("%Y-%m-%d %H:%M:%S") if config.end_at else "(未指定)"
    return (
        "[collect][period][global] 入力=日時範囲。"
        f"指定期間={start_text} ～ {end_text} -> 正規化後対象年={normalized_text} / 取得期間={normalized_period_text} "
        "理由=取得処理は年単位ジョブのため、開始年〜終了年へ展開"
    )


def format_station_target_period_log(source: str, station_key: str, years: list[int], reason: str) -> str:
    years_text = format_years_list_text(years)
    period_text = format_years_period_text(years)
    return (
        f"[collect][period][station] source={source} 観測所={station_key} "
        f"対象年={years_text} / 取得期間={period_text} 理由={reason}"
    )


def format_years_list_text(years: list[int]) -> str:
    return ", ".join(str(year) for year in years) if years else "(なし)"


def format_years_period_text(years: list[int]) -> str:
    if not years:
        return "(なし)"
    start_year = min(years)
    end_year = max(years)
    return f"{start_year}-01-01 00:00:00 ～ {end_year}-12-31 23:59:59"


def resolve_query_period(config: "RainfallRunInput") -> tuple[datetime, datetime]:
    if config.start_at is not None and config.end_at is not None:
        if config.year is not None or config.years:
            raise ValueError("Specify either year/years or start_at/end_at, not both")
        return config.start_at, config.end_at

    years = config.years if config.years else ([config.year] if config.year else [])
    if years:
        start_year = min(years)
        end_year = max(years)
        if start_year < 1900 or end_year > 2100:
            raise ValueError(f"Unsupported year range: {start_year}-{end_year}")
        return datetime(start_year, 1, 1, 0, 0, 0), datetime(end_year, 12, 31, 23, 59, 59)

    if config.start_at is None and config.end_at is None:
        raise ValueError("start_at/end_at or year(s) is required")
    raise ValueError("Both start_at and end_at are required when year is not specified")
