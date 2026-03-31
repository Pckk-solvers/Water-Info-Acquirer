from __future__ import annotations

import json

from hydrology_graphs.io.style_store import default_style, load_style, save_style


def test_load_style_accepts_legacy_title_template_and_save_is_canonical(tmp_path):
    payload = default_style()
    payload["graph_styles"]["hyetograph"].pop("title")
    payload["graph_styles"]["hyetograph"]["title_template"] = "{station_name} legacy"

    result = load_style(payload=payload)
    assert result.is_valid
    assert result.style["graph_styles"]["hyetograph"]["title"]["template"] == "{station_name} legacy"

    out = tmp_path / "style.json"
    save_style(out, result.style)
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert "title_template" not in json.dumps(saved, ensure_ascii=False)
    assert saved["schema_version"] == "1.0"


def test_load_style_rejects_invalid_schema():
    payload = default_style()
    payload["schema_version"] = "0.9"

    result = load_style(payload=payload)

    assert not result.is_valid
    assert any(message.startswith("error:unsupported_schema_version") for message in result.warnings)
