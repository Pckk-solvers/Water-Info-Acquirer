from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.sources.jma.station_index import (
    resolve_jma_station_codes_from_prefectures,
    resolve_jma_stations_from_prefectures,
)


def _mock_index() -> dict:
    return {
        "by_block_no": {
            "62001": [
                {
                    "prec_no": "27",
                    "pref_name": "大阪府",
                    "block_no": "62001",
                    "station_name": "大阪",
                    "obs_type": "s1",
                }
            ],
            "61286": [
                {
                    "prec_no": "26",
                    "pref_name": "京都府",
                    "block_no": "61286",
                    "station_name": "京都",
                    "obs_type": "s1",
                }
            ],
            "63518": [
                {
                    "prec_no": "28",
                    "pref_name": "兵庫県",
                    "block_no": "63518",
                    "station_name": "神戸",
                    "obs_type": "s1",
                }
            ],
            "65042": [
                {
                    "prec_no": "30",
                    "pref_name": "和歌山県",
                    "block_no": "65042",
                    "station_name": "和歌山",
                    "obs_type": "s1",
                }
            ],
            "64036": [
                {
                    "prec_no": "29",
                    "pref_name": "奈良県",
                    "block_no": "64036",
                    "station_name": "奈良",
                    "obs_type": "s1",
                }
            ],
            "47639": [
                {
                    "prec_no": "49",
                    "pref_name": "山梨県",
                    "block_no": "47639",
                    "station_name": "富士山",
                    "obs_type": "s1",
                },
                {
                    "prec_no": "50",
                    "pref_name": "静岡県",
                    "block_no": "47639",
                    "station_name": "富士山",
                    "obs_type": "s1",
                },
            ],
        }
    }


def test_resolve_station_codes_from_prefecture_names():
    codes, unknown = resolve_jma_station_codes_from_prefectures(
        ["大阪", "京都", "兵庫", "和歌山", "奈良"],
        index_data=_mock_index(),
    )
    assert unknown == []
    assert codes == ["61286", "62001", "63518", "64036", "65042"]


def test_resolve_stations_from_prefecture_uses_exact_prefecture_for_duplicate_code():
    stations, unknown = resolve_jma_stations_from_prefectures(
        ["静岡"],
        index_data=_mock_index(),
    )
    assert unknown == []
    assert len(stations) == 1
    assert stations[0].prefecture_code == "50"
    assert stations[0].block_number == "47639"


def test_resolve_stations_from_prefecture_reports_unknown():
    stations, unknown = resolve_jma_stations_from_prefectures(
        ["大阪", "不存在県"],
        index_data=_mock_index(),
    )
    assert len(stations) == 1
    assert unknown == ["不存在県"]
