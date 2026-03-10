
# jma_codes_fetcher.py

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Tuple

from bs4 import BeautifulSoup

from jma_rainfall_pipeline.utils.cache_manager import CACHE_MANAGER, CacheEntry
from jma_rainfall_pipeline.utils.http_client import throttled_get

# Base URL
BASE_URL = "https://www.data.jma.go.jp/obd"

logger = logging.getLogger(__name__)

VIEWPOINT_RE = re.compile(r"viewPoint\((.*)\)\s*;?\s*$", re.DOTALL)
VIEWPOINT_ARGS_RE = re.compile(r"'([^']*)'|([^,]+)")


def fetch_prefecture_codes(timeout: int = 10) -> List[Tuple[str, str]]:
    """Fetch the list of prefecture codes and names with persistent caching."""
    cache_entry: CacheEntry | None = CACHE_MANAGER.load_prefectures()
    if cache_entry and not cache_entry.expired:
        return list(cache_entry.data)

    stale_data = list(cache_entry.data) if cache_entry else None

    try:
        data = _download_prefecture_codes(timeout)
    except Exception as exc:  # pragma: no cover - fallback path
        if stale_data is not None:
            logger.warning("Using cached prefecture list due to fetch failure: %s", exc)
            return stale_data
        raise

    CACHE_MANAGER.save_prefectures(data)
    return data


def fetch_station_codes(prec_no: str, timeout: int = 10) -> List[Dict[str, Any]]:
    """Fetch station metadata for a prefecture with persistent caching."""
    cache_entry: CacheEntry | None = CACHE_MANAGER.load_stations(prec_no)
    if cache_entry and not cache_entry.expired:
        return list(cache_entry.data)

    stale_data = list(cache_entry.data) if cache_entry else None

    try:
        data = _download_station_codes(prec_no, timeout)
    except Exception as exc:  # pragma: no cover - fallback path
        if stale_data is not None:
            logger.warning(
                "Using cached station list for prefecture %s due to fetch failure: %s",
                prec_no,
                exc,
            )
            return stale_data
        raise

    CACHE_MANAGER.save_stations(prec_no, data)
    return data


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _download_prefecture_codes(timeout: int) -> List[Tuple[str, str]]:
    url = (
        f"{BASE_URL}/stats/etrn/select/"
        "prefecture00.php?prec_no=&block_no=&year=&month=&day=&view="
    )
    resp = throttled_get(url, timeout=timeout, rate_limit=False)
    soup = BeautifulSoup(resp.text, "html.parser")

    map_tag = soup.find("map")
    if not map_tag:
        raise RuntimeError("prefecture map tag not found in response")

    result: List[Tuple[str, str]] = []
    for area in map_tag.find_all("area"):
        href = area.get("href", "")
        m_prec = re.search(r"prec_no=(\d+)", href)
        m_block = re.search(r"block_no=(\d*)", href)
        if m_prec and m_block and m_block.group(1) == "":
            code = m_prec.group(1)
            name = area.get("alt", "").strip()
            result.append((code, name))
    return result


def _download_station_codes(prec_no: str, timeout: int) -> List[Dict[str, Any]]:
    url = (
        f"{BASE_URL}/stats/etrn/select/prefecture.php"
        f"?prec_no={prec_no}&block_no=&year=&month=&day=&view="
    )
    resp = throttled_get(url, timeout=timeout, rate_limit=False)
    soup = BeautifulSoup(resp.text, "html.parser")

    maps = soup.find_all("map")
    station_map = None
    for m in maps:
        if any(re.search(r"block_no=\d+", area.get("href", "")) for area in m.find_all("area")):
            station_map = m
            break
    if station_map is None:
        raise RuntimeError(f"station map not found for prefecture {prec_no}")

    stations: List[Dict[str, Any]] = []
    seen = set()
    for area in station_map.find_all("area"):
        href = area.get("href", "")
        m_block = re.search(r"block_no=(\d+)", href)
        if not m_block:
            continue
        block_no = m_block.group(1)
        if block_no == "00" or block_no in seen:
            continue
        seen.add(block_no)
        name = area.get("alt", "").strip()
        on_mouse = area.get("onmouseover", "")
        m_method = re.search(r"viewPoint\('([a-z])'", on_mouse)
        obs_method = m_method.group(1) if m_method else ""
        meta = _parse_viewpoint_metadata(on_mouse)
        stations.append({
            "block_no": block_no,
            "station": name,
            "obs_method": obs_method,
            "station_kana": meta.get("station_kana", ""),
            "latitude": meta.get("latitude", ""),
            "longitude": meta.get("longitude", ""),
        })
    return stations


def _parse_viewpoint_metadata(on_mouse: str) -> Dict[str, str]:
    text = str(on_mouse or "").strip()
    matched = VIEWPOINT_RE.search(text)
    if not matched:
        return {}

    raw_args = matched.group(1)
    args: list[str] = []
    for token in VIEWPOINT_ARGS_RE.finditer(raw_args):
        if token.group(1) is not None:
            args.append(token.group(1))
        else:
            args.append(str(token.group(2) or "").strip())

    if len(args) < 8:
        return {}

    station_kana = str(args[3]).strip()
    latitude = _to_decimal_text(args[4], args[5])
    longitude = _to_decimal_text(args[6], args[7])
    return {
        "station_kana": station_kana,
        "latitude": latitude,
        "longitude": longitude,
    }


def _to_decimal_text(deg_text: str, min_text: str) -> str:
    try:
        deg = float(str(deg_text).strip())
        minute = float(str(min_text).strip())
    except ValueError:
        return ""
    return f"{deg + minute / 60.0:.6f}"


if __name__ == "__main__":
    # Example usage for manual testing
    import pprint

    prefs = fetch_prefecture_codes()
    pprint.pprint(prefs)
    stations = fetch_station_codes('11')
    pprint.pprint(stations)
