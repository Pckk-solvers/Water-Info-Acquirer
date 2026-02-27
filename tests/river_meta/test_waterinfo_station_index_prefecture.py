from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.waterinfo_station_index import resolve_waterinfo_station_codes_from_prefectures


def test_resolve_waterinfo_station_codes_from_prefectures(monkeypatch):
    pref_options = [
        ("2701", "大阪府"),
        ("2601", "京都府"),
        ("2801", "兵庫県"),
    ]
    komoku_options = [("01", "雨量")]
    ids_by_pref = {
        "2701": ["2700000001", "2700000002"],
        "2601": ["2600000001", "2600000002"],
    }

    monkeypatch.setattr(
        "river_meta.rainfall.waterinfo_station_index.build_session",
        lambda user_agent: object(),
    )
    monkeypatch.setattr(
        "river_meta.rainfall.waterinfo_station_index.fetch_master_options",
        lambda session, timeout: (pref_options, komoku_options),
    )

    def _fake_collect(
        session,
        *,
        params,
        timeout,
        sleep_sec,
        page_max,
        warn_log,
    ):
        codes = ids_by_pref.get(params["KEN"], [])
        return codes, len(codes)

    monkeypatch.setattr("river_meta.rainfall.waterinfo_station_index.collect_station_ids", _fake_collect)

    codes, unknown = resolve_waterinfo_station_codes_from_prefectures(
        ["大阪", "京都", "不存在県"],
        timeout=0.1,
        sleep_sec=0.0,
        page_max=1,
    )
    assert unknown == ["不存在県"]
    assert codes == ["2600000001", "2600000002", "2700000001", "2700000002"]
