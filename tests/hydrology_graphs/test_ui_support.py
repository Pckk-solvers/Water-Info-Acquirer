from __future__ import annotations

from datetime import date

from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.services.dto import PrecheckItem
from hydrology_graphs.ui.view_models import (
    build_batch_targets,
    build_preview_choices,
    format_result_target_display,
    format_result_target_display_from_target_id,
    format_result_status_display,
    graph_targets_from_precheck_items,
    parse_base_dates_text,
    selected_station_pairs,
)
from hydrology_graphs.ui.execute_actions import refresh_preview_choices


def test_selected_station_pairs_deduplicates():
    stations = [
        ("jma", "001", "A"),
        ("jma", "001", "A"),
        ("water_info", "001", "A"),
    ]
    pairs = selected_station_pairs(stations, [0, 1, 2])
    assert pairs == [("jma", "001"), ("water_info", "001")]


def test_parse_base_dates_text_removes_blank_lines():
    dates = parse_base_dates_text("2026-01-01\n\n 2026-01-02 \n")
    assert dates == ["2026-01-01", "2026-01-02"]


def test_graph_targets_from_precheck_items_uses_ok_only():
    items = [
        PrecheckItem(
            target_id="1",
            source="jma",
            station_key="001",
            graph_type="hyetograph",
            base_datetime="2026-01-01",
            status="ok",
            event_window_days=5,
        ),
        PrecheckItem(
            target_id="2",
            source="jma",
            station_key="001",
            graph_type="annual_max_rainfall",
            base_datetime=None,
            status="ng",
        ),
    ]
    targets = graph_targets_from_precheck_items(items=items)
    assert len(targets) == 1
    assert targets[0].base_date == date(2026, 1, 1)
    assert targets[0].event_window_days == 5


def test_build_preview_choices_creates_display_maps():
    ok_targets = [
        GraphTarget(source="jma", station_key="001", graph_type="hyetograph", base_date=date(2026, 1, 1), event_window_days=3),
        GraphTarget(source="jma", station_key="001", graph_type="hydrograph_discharge", base_date=date(2026, 1, 2), event_window_days=3),
    ]
    stations = [("jma", "001", "観測所A")]
    choices = build_preview_choices(
        ok_targets=ok_targets,
        catalog_stations=stations,
        graph_key_to_display={"hyetograph": "ハイエトグラフ（雨量）", "hydrograph_discharge": "ハイドログラフ（流量）"},
    )
    assert choices.station_values == ["観測所A (jma:001)"]
    assert choices.date_values == ["2026-01-01", "2026-01-02"]
    assert set(choices.graph_values) == {"ハイエトグラフ（雨量）", "ハイドログラフ（流量）"}


def test_build_batch_targets_converts_graph_target():
    targets = [
        GraphTarget(source="jma", station_key="001", graph_type="hyetograph", base_date=date(2026, 1, 1), event_window_days=3),
    ]
    batch_targets = build_batch_targets(targets)
    assert batch_targets[0].base_datetime == "2026-01-01"
    assert batch_targets[0].event_window_days == 3


def test_format_result_target_display_uses_japanese_labels():
    text = format_result_target_display(
        source="jma",
        station_key="111",
        graph_type="hydrograph_water_level",
        base_datetime="2025-01-02",
        event_window_days=3,
        catalog_stations=[("jma", "111", "高幡橋")],
        source_label_map={"jma": "気象庁"},
        graph_label_map={"hydrograph_water_level": "ハイドログラフ（水位）"},
    )
    assert text == "高幡橋（気象庁:111） / ハイドログラフ（水位） / 2025-01-02 / 3日窓"


def test_format_result_target_display_from_target_id_uses_japanese_labels():
    text = format_result_target_display_from_target_id(
        "jma:111:hydrograph_water_level:2025-01-02:3day",
        catalog_stations=[("jma", "111", "高幡橋")],
        source_label_map={"jma": "気象庁"},
        graph_label_map={"hydrograph_water_level": "ハイドログラフ（水位）"},
    )
    assert text == "高幡橋（気象庁:111） / ハイドログラフ（水位） / 2025-01-02 / 3日窓"


def test_format_result_status_display_uses_japanese_labels():
    assert format_result_status_display("ready") == "準備完了"
    assert format_result_status_display("precheck_ng") == "要確認"
    assert format_result_status_display("running") == "実行中"
    assert format_result_status_display("success") == "完了"
    assert format_result_status_display("failed") == "失敗"
    assert format_result_status_display("skipped") == "スキップ"


class _DummyVar:
    def __init__(self, value: str = "") -> None:
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


class _DummyCombo:
    def __init__(self) -> None:
        self.values = None

    def configure(self, **kwargs):
        if "values" in kwargs:
            self.values = list(kwargs["values"])


def test_refresh_preview_choices_resets_invalid_selection_to_first_choice():
    app = type("DummyApp", (), {})()
    app._precheck_ok_targets = [
        GraphTarget(source="jma", station_key="001", graph_type="hyetograph", base_date=date(2026, 1, 1), event_window_days=3),
        GraphTarget(source="jma", station_key="002", graph_type="hydrograph_discharge", base_date=date(2026, 1, 2), event_window_days=3),
    ]
    app._catalog_stations = [("jma", "001", "観測所A"), ("jma", "002", "観測所B")]
    app._preview_graph_key_to_display = {
        "hyetograph": "ハイエトグラフ（雨量）",
        "hydrograph_discharge": "ハイドログラフ（流量）",
    }
    app.preview_station_combo = _DummyCombo()
    app.preview_date_combo = _DummyCombo()
    app.preview_graph_combo = _DummyCombo()
    app.preview_target_station = _DummyVar("無効な観測所")
    app.preview_target_date = _DummyVar("2099-12-31")
    app.preview_target_graph = _DummyVar("無効な種別")
    app._refresh_style_forms_from_payload = lambda: None

    refresh_preview_choices(app)

    assert app.preview_target_station.get() == "観測所A (jma:001)"
    assert app.preview_target_date.get() == "2026-01-01"
    assert app.preview_target_graph.get() == "ハイドログラフ（流量）"
