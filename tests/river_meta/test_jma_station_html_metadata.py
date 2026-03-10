from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import _parse_viewpoint_metadata
from river_meta.rainfall.commands.build_jma_station_index import build_jma_station_index


def test_parse_viewpoint_metadata_extracts_latlon() -> None:
    on_mouse = (
        "javascript:viewPoint('s','47401','稚内','ワツカナイ','45','24.9','141','40.7',"
        "'2.8','1','1','1','1','1','1','9999','99','99','','','','','');"
    )

    result = _parse_viewpoint_metadata(on_mouse)

    assert result["station_kana"] == "ワツカナイ"
    assert result["latitude"] == "45.415000"
    assert result["longitude"] == "141.678333"


def test_build_jma_station_index_includes_html_latlon(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "river_meta.rainfall.commands.build_jma_station_index.fetch_prefecture_codes",
        lambda: [("11", "北海道")],
    )
    monkeypatch.setattr(
        "river_meta.rainfall.commands.build_jma_station_index.fetch_station_codes",
        lambda prec_no: [
            {
                "block_no": "47401",
                "station": "稚内",
                "station_kana": "ワツカナイ",
                "obs_method": "s",
                "latitude": "45.415000",
                "longitude": "141.678333",
            }
        ],
    )

    out_path = tmp_path / "jma_station_index.json"
    build_jma_station_index(output_path=str(out_path))
    text = out_path.read_text(encoding="utf-8")

    assert '"latitude": "45.415000"' in text
    assert '"longitude": "141.678333"' in text
    assert '"station_name_kana": "ワツカナイ"' in text
