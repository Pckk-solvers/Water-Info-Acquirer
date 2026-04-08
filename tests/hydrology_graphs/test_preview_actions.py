from __future__ import annotations

from datetime import date

from hydrology_graphs.ui.preview_actions import _build_sample_output_path
from hydrology_graphs.ui.preview_actions import _build_preview_input


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


class _DummyVar:
    def __init__(self, value: str = "") -> None:
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


def test_build_preview_input_reads_time_display_mode_from_payload():
    app = type("DummyApp", (), {})()
    app._style_payload = {
        "display": {"time_display_mode": "24h"},
        "graph_styles": {"hyetograph:3day": {}},
    }
    app._style_json_path = None
    app.parquet_dir = _DummyVar("/tmp/input")
    app.threshold_path = _DummyVar("")
    app.preview_target_station = _DummyVar("観測所A (jma:001)")
    app._preview_station_display_to_pair = {"観測所A (jma:001)": ("jma", "001")}
    app.preview_target_date = _DummyVar("2026-01-02")
    app.preview_target_graph = _DummyVar("ハイエトグラフ（雨量） 3日")
    app._preview_graph_display_to_key = {"ハイエトグラフ（雨量） 3日": "hyetograph:3day"}
    app._current_preview_graph_key = lambda: app._preview_graph_display_to_key.get(
        app.preview_target_graph.get(),
        app.preview_target_graph.get(),
    )
    app.preview_message = _DummyVar("")
    app._precheck_ok_targets = [
        type("Target", (), {"source": "jma", "station_key": "001", "graph_type": "hyetograph", "event_window_days": 3, "base_date": date(2026, 1, 2)})(),
    ]
    app._build_preview_style_payload = lambda payload: payload
    app._style_from_editor = lambda silent=False: None
    app._apply_style_form_values = lambda: True
    app._set_style_text_from_payload = lambda: None
    app._push_style_history = lambda payload: None

    preview_input, threshold_file = _build_preview_input(app, silent_json_error=True)

    assert threshold_file is None
    assert preview_input is not None
    assert preview_input.time_display_mode == "24h"
    assert preview_input.event_window_terminal_padding is True


def test_build_preview_input_enables_terminal_padding_for_datetime_mode():
    app = type("DummyApp", (), {})()
    app._style_payload = {
        "display": {"time_display_mode": "datetime"},
        "graph_styles": {"hyetograph:3day": {}},
    }
    app._style_json_path = None
    app.parquet_dir = _DummyVar("/tmp/input")
    app.threshold_path = _DummyVar("")
    app.preview_target_station = _DummyVar("観測所A (jma:001)")
    app._preview_station_display_to_pair = {"観測所A (jma:001)": ("jma", "001")}
    app.preview_target_date = _DummyVar("2026-01-02")
    app.preview_target_graph = _DummyVar("ハイエトグラフ（雨量） 3日")
    app._preview_graph_display_to_key = {"ハイエトグラフ（雨量） 3日": "hyetograph:3day"}
    app._current_preview_graph_key = lambda: app._preview_graph_display_to_key.get(
        app.preview_target_graph.get(),
        app.preview_target_graph.get(),
    )
    app.preview_message = _DummyVar("")
    app._precheck_ok_targets = [
        type("Target", (), {"source": "jma", "station_key": "001", "graph_type": "hyetograph", "event_window_days": 3, "base_date": date(2026, 1, 2)})(),
    ]
    app._build_preview_style_payload = lambda payload: payload
    app._style_from_editor = lambda silent=False: None
    app._apply_style_form_values = lambda: True
    app._set_style_text_from_payload = lambda: None
    app._push_style_history = lambda payload: None

    preview_input, threshold_file = _build_preview_input(app, silent_json_error=True)

    assert threshold_file is None
    assert preview_input is not None
    assert preview_input.time_display_mode == "datetime"
    assert preview_input.event_window_terminal_padding is True


def test_build_preview_input_rejects_mismatched_precheck_target():
    app = type("DummyApp", (), {})()
    app._style_payload = {
        "display": {"time_display_mode": "datetime"},
        "graph_styles": {"hyetograph:3day": {}},
    }
    app._style_json_path = None
    app.parquet_dir = _DummyVar("/tmp/input")
    app.threshold_path = _DummyVar("")
    app.preview_target_station = _DummyVar("観測所A (jma:001)")
    app.preview_target_date = _DummyVar("2026-01-03")
    app.preview_target_graph = _DummyVar("ハイエトグラフ（雨量） 3日")
    app._preview_station_display_to_pair = {
        "観測所A (jma:001)": ("jma", "001"),
        "観測所B (jma:002)": ("jma", "002"),
    }
    app._preview_graph_display_to_key = {"ハイエトグラフ（雨量） 3日": "hyetograph:3day"}
    app._current_preview_graph_key = lambda: app._preview_graph_display_to_key.get(
        app.preview_target_graph.get(),
        app.preview_target_graph.get(),
    )
    app._precheck_ok_targets = [
        type("Target", (), {"source": "jma", "station_key": "001", "graph_type": "hyetograph", "event_window_days": 3, "base_date": date(2026, 1, 2)})(),
        type("Target", (), {"source": "jma", "station_key": "002", "graph_type": "hyetograph", "event_window_days": 3, "base_date": date(2026, 1, 4)})(),
    ]
    app._build_preview_style_payload = lambda payload: payload
    app._style_from_editor = lambda silent=False: None
    app._apply_style_form_values = lambda: True
    app._set_style_text_from_payload = lambda: None
    app._push_style_history = lambda payload: None
    app.preview_message = _DummyVar("")

    built = _build_preview_input(app, silent_json_error=True)

    assert built is None
    assert app.preview_message.value == "選択した観測所・基準日・対象グラフに一致するプレビュー候補がありません。"
