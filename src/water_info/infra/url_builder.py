"""URL builders for water_info."""

from __future__ import annotations


def build_hourly_base(mode_type: str) -> tuple[str, str]:
    if mode_type == "S":
        return "2", "Water"
    if mode_type == "R":
        return "6", "Water"
    if mode_type == "U":
        return "2", "Rain"
    raise ValueError("mode_typeは 'S', 'R', または 'U' を指定してください。")


def build_daily_base(mode_type: str) -> tuple[str, str, str, str]:
    if mode_type == "S":
        return "3", "水位", "水位[m]", "_WD.xlsx"
    if mode_type == "R":
        return "7", "流量", "流量[m^3/s]", "_QD.xlsx"
    if mode_type == "U":
        return "3", "雨量", "雨量[mm/h]", "_RD.xlsx"
    raise ValueError("mode_typeは 'S', 'R', または 'U' を指定してください。")


def build_hourly_url(code: str, kind: str, mode_str: str, bgn_date: str, end_date: str) -> str:
    return (
        f"http://www1.river.go.jp/cgi-bin/Dsp{mode_str}Data.exe"
        f"?KIND={kind}&ID={code}&BGNDATE={bgn_date}&ENDDATE={end_date}&KAWABOU=NO"
    )


def build_daily_base_url(mode_type: str) -> str:
    if mode_type in ("S", "R"):
        return "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?"
    if mode_type == "U":
        return "http://www1.river.go.jp/cgi-bin/DspRainData.exe?"
    raise ValueError("mode_typeは 'S', 'R', または 'U' を指定してください。")


def build_daily_url(base_url: str, code: str, kind: str, bgn_date: str, end_date: str) -> str:
    return f"{base_url}KIND={kind}&ID={code}&BGNDATE={bgn_date}&ENDDATE={end_date}&KAWABOU=NO"
