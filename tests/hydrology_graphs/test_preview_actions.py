from __future__ import annotations

from hydrology_graphs.ui.preview_actions import _build_sample_output_path


def test_build_sample_output_path_contains_target_parts():
    path = _build_sample_output_path(
        graph_type="hyetograph",
        event_window_days=3,
        station_key="ABC001",
        base_datetime="2026-01-02",
    )
    text = str(path)
    assert "outputs" in text
    assert "dev_preview_samples" in text
    assert "ABC001" in text
    assert "hyetograph_3day" in text
    assert "2026-01-02" in text
