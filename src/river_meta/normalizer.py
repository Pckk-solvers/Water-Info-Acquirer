from __future__ import annotations

import re

from .models import SiteMeta


CSV_COLUMNS = [
    "station_id",
    "station_name",
    "suikei_mei",
    "kasen_mei",
    "location",
    "latitude",
    "longitude",
    "kanrisha",
    "daily_years_count",
    "hourly_years_count",
    "daily_years",
    "hourly_years",
    "status",
    "error_count",
    "warn_count",
    "error_message",
]


KEY_CANDIDATES = {
    "station_name": ("観測所名", "観測所", "局名"),
    "river_system": ("水系名", "水系"),
    "river_name": ("河川名", "河川"),
    "location": ("所在地", "住所"),
    "latitude": ("緯度",),
    "longitude": ("経度",),
    "administrator": ("管理者", "管理事務所", "管理"),
    "latlon": ("緯度経度",),
}


def _normalize_text(value: str) -> str:
    normalized = value.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _value_to_text(value: str | list[str]) -> str:
    if isinstance(value, list):
        return " / ".join(_normalize_text(v) for v in value if _normalize_text(v))
    return _normalize_text(value)


def _search_value(
    meta: SiteMeta,
    candidates: tuple[str, ...],
    *,
    allow_partial: bool = True,
    exclude_keys: tuple[str, ...] = (),
) -> str:
    for key in candidates:
        if key in meta:
            text = _value_to_text(meta[key])
            if text:
                return text

    if not allow_partial:
        return ""

    for key in candidates:
        for meta_key, raw_value in meta.items():
            if any(excluded in meta_key for excluded in exclude_keys):
                continue
            if key in meta_key:
                text = _value_to_text(raw_value)
                if text:
                    return text
    return ""


def _split_latlon(text: str) -> tuple[str, str]:
    normalized = _normalize_text(text)
    directional = re.search(r"(北緯[^東西南北]+)\s+(東経.+)", normalized)
    if directional:
        return directional.group(1).strip(), directional.group(2).strip()

    directional = re.search(r"(南緯[^東西南北]+)\s+(西経.+)", normalized)
    if directional:
        return directional.group(1).strip(), directional.group(2).strip()

    decimal_pair = re.search(
        r"([+-]?\d+(?:\.\d+)?)\s*[,/ ]\s*([+-]?\d+(?:\.\d+)?)",
        normalized,
    )
    if decimal_pair:
        return decimal_pair.group(1), decimal_pair.group(2)

    if "," in text:
        left, right = [segment.strip() for segment in text.split(",", maxsplit=1)]
        return left, right
    if " " in text:
        left, right = [segment.strip() for segment in text.split(" ", maxsplit=1)]
        return left, right
    return text.strip(), ""


def extract_csv_meta_fields(site_meta: SiteMeta) -> dict[str, str]:
    fields = {
        "station_name": _search_value(site_meta, KEY_CANDIDATES["station_name"]),
        "suikei_mei": _search_value(site_meta, KEY_CANDIDATES["river_system"]),
        "kasen_mei": _search_value(site_meta, KEY_CANDIDATES["river_name"]),
        "location": _search_value(site_meta, KEY_CANDIDATES["location"]),
        "latitude": _search_value(
            site_meta,
            KEY_CANDIDATES["latitude"],
            exclude_keys=("緯度経度",),
        ),
        "longitude": _search_value(
            site_meta,
            KEY_CANDIDATES["longitude"],
            exclude_keys=("緯度経度",),
        ),
        "kanrisha": _search_value(site_meta, KEY_CANDIDATES["administrator"]),
    }

    latlon = _search_value(site_meta, KEY_CANDIDATES["latlon"])
    if latlon:
        if not fields["latitude"] or not fields["longitude"]:
            latitude, longitude = _split_latlon(latlon)
            fields["latitude"] = fields["latitude"] or latitude
            fields["longitude"] = fields["longitude"] or longitude

    return fields
