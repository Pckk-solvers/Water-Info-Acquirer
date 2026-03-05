from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.station_index import (
    resolve_jma_stations_from_codes,
)


def _mock_index() -> dict:
    return {
        "by_block_no": {
            "47401": [
                {
                    "prec_no": "27",
                    "pref_name": "大阪府",
                    "block_no": "47401",
                    "station_name": "大阪",
                    "obs_type": "s1",
                    "station_id": "62078",
                }
            ],
            "0364": [
                {
                    "prec_no": "26",
                    "pref_name": "京都府",
                    "block_no": "0364",
                    "station_name": "京都",
                    "obs_type": "a1",
                    "station_id": "61286", # fake but valid for test
                }
            ]
        }
    }


def test_resolve_jma_stations_from_codes_block_no():
    stations, issues = resolve_jma_stations_from_codes(
        ["47401", "0364"],
        index_data=_mock_index(),
    )
    assert not issues
    assert len(stations) == 2
    assert stations[0].prefecture_code == "27"
    assert stations[0].block_number == "47401"
    assert stations[1].prefecture_code == "26"
    assert stations[1].block_number == "0364"


def test_resolve_jma_stations_from_codes_station_id():
    # Pass AMeDAS 5-digit station_id instead of block_no
    stations, issues = resolve_jma_stations_from_codes(
        ["62078", "61286"],
        index_data=_mock_index(),
    )
    assert not issues
    assert len(stations) == 2
    
    # 62078 should resolve to 大阪 (block_no 47401)
    osaka = next(s for s in stations if s.station_name == "大阪")
    assert osaka.prefecture_code == "27"
    assert osaka.block_number == "47401"
    
    # 61286 should resolve to 京都 (block_no 0364)
    kyoto = next(s for s in stations if s.station_name == "京都")
    assert kyoto.prefecture_code == "26"
    assert kyoto.block_number == "0364"


def test_resolve_jma_stations_from_codes_not_found():
    stations, issues = resolve_jma_stations_from_codes(
        ["99999"],
        index_data=_mock_index(),
    )
    assert not stations
    assert len(issues) == 1
    assert issues[0].code == "99999"
    assert issues[0].reason == "not_found"


def test_resolve_jma_stations_from_codes_parenthesized_display_format():
    stations, issues = resolve_jma_stations_from_codes(
        ["47401 (62078)", "0364（61286）"],
        index_data=_mock_index(),
    )
    assert not issues
    assert len(stations) == 2
    assert {s.block_number for s in stations} == {"47401", "0364"}


def test_resolve_jma_stations_from_codes_parenthesized_station_id_only():
    stations, issues = resolve_jma_stations_from_codes(
        ["(62078)"],
        index_data=_mock_index(),
    )
    assert not issues
    assert len(stations) == 1
    assert stations[0].block_number == "47401"
