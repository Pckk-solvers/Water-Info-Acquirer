from __future__ import annotations

from datetime import date

from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.services.dto import PrecheckItem
from hydrology_graphs.ui.app import HydrologyGraphsApp
from hydrology_graphs.ui.view_models import (
    build_batch_targets,
    build_preview_choices,
    format_station_display_text,
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
        graph_key_to_display={
            "hyetograph:3day": "ハイエトグラフ（雨量） 3日",
            "hydrograph_discharge:3day": "ハイドログラフ（流量） 3日",
        },
    )
    assert choices.station_values == ["観測所A (jma:001)"]
    assert choices.date_values == ["2026-01-01", "2026-01-02"]
    assert set(choices.graph_values) == {"ハイエトグラフ（雨量） 3日", "ハイドログラフ（流量） 3日"}


def test_build_preview_choices_filters_graphs_by_station_and_date():
    ok_targets = [
        GraphTarget(source="jma", station_key="001", graph_type="hyetograph", base_date=date(2026, 1, 1), event_window_days=3),
        GraphTarget(source="jma", station_key="001", graph_type="hydrograph_discharge", base_date=date(2026, 1, 2), event_window_days=3),
        GraphTarget(source="jma", station_key="002", graph_type="hydrograph_water_level", base_date=date(2026, 1, 2), event_window_days=3),
    ]
    choices = build_preview_choices(
        ok_targets=ok_targets,
        catalog_stations=[("jma", "001", "観測所A"), ("jma", "002", "観測所B")],
        graph_key_to_display={
            "hyetograph:3day": "ハイエトグラフ（雨量） 3日",
            "hydrograph_discharge:3day": "ハイドログラフ（流量） 3日",
            "hydrograph_water_level:3day": "ハイドログラフ（水位） 3日",
        },
        selected_station_pair=("jma", "001"),
        selected_base_date="2026-01-02",
    )
    assert choices.date_values == ["2026-01-01", "2026-01-02"]
    assert choices.graph_values == ["ハイドログラフ（流量） 3日"]


def test_build_preview_choices_keeps_annual_graph_when_date_selected():
    ok_targets = [
        GraphTarget(source="jma", station_key="001", graph_type="hyetograph", base_date=date(2026, 1, 2), event_window_days=3),
        GraphTarget(source="jma", station_key="001", graph_type="annual_max_rainfall", base_date=None, event_window_days=None),
    ]
    choices = build_preview_choices(
        ok_targets=ok_targets,
        catalog_stations=[("jma", "001", "観測所A")],
        graph_key_to_display={
            "hyetograph:3day": "ハイエトグラフ（雨量） 3日",
            "annual_max_rainfall": "年最大雨量",
        },
        selected_station_pair=("jma", "001"),
        selected_base_date="2026-01-02",
    )
    assert set(choices.graph_values) == {"ハイエトグラフ（雨量） 3日", "年最大雨量"}


def test_format_station_display_text_includes_metric_labels():
    text = format_station_display_text(
        source="water_info",
        station_key="303",
        station_name="高幡橋",
        checked=False,
        source_label_map={"jma": "気象庁", "water_info": "水文水質DB"},
        metric_labels=("流量", "水位"),
    )
    assert text == "☐ 水文水質DB:303 (高幡橋) / 流量 / 水位"


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
    assert format_result_status_display("warn") == "欠測あり（継続可）"
    assert format_result_status_display("precheck_warn") == "欠測あり（継続可）"
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
        "hyetograph:3day": "ハイエトグラフ（雨量） 3日",
        "hydrograph_discharge:3day": "ハイドログラフ（流量） 3日",
    }
    app._preview_station_display_to_pair = {}
    app.preview_station_combo = _DummyCombo()
    app.preview_date_combo = _DummyCombo()
    app.preview_graph_combo = _DummyCombo()
    app.preview_target_station = _DummyVar("無効な観測所")
    app.preview_target_date = _DummyVar("2099-12-31")
    app.preview_target_graph = _DummyVar("無効な種別")
    app._preview_graph_display_to_key = {
        "ハイエトグラフ（雨量） 3日": "hyetograph:3day",
        "ハイドログラフ（流量） 3日": "hydrograph_discharge:3day",
    }
    app._refresh_style_forms_from_payload = lambda: None

    refresh_preview_choices(app)

    assert app.preview_target_station.get() == "観測所A (jma:001)"
    assert app.preview_target_date.get() == "2026-01-01"
    assert app.preview_target_graph.get() == "ハイエトグラフ（雨量） 3日"


def test_ordered_style_fields_uses_fixed_path_order():
    fields = [
        {"path": "x_axis.range_margin_rate", "label": "X軸範囲マージン率"},
        {"path": "figure_width", "label": "図幅(inch)"},
        {"path": "title.template", "label": "タイトルテンプレート"},
    ]

    ordered = HydrologyGraphsApp._ordered_style_fields(fields)

    assert [field["path"] for field in ordered] == ["figure_width", "title.template", "x_axis.range_margin_rate"]


def test_should_use_palette_row_for_color_or_many_fields():
    color_row = {
        "values": [
            {"path": "series_color", "kind": "color"},
            {"path": "series_width", "kind": "float"},
        ]
    }
    many_values_row = {
        "values": [
            {"path": "a", "kind": "float"},
            {"path": "b", "kind": "float"},
            {"path": "c", "kind": "float"},
        ]
    }
    compact_row = {
        "values": [
            {"path": "x_axis.data_trim_start_hours", "kind": "float"},
            {"path": "x_axis.data_trim_end_hours", "kind": "float"},
        ]
    }

    assert HydrologyGraphsApp._should_use_palette_row(color_row) is True
    assert HydrologyGraphsApp._should_use_palette_row(many_values_row) is True
    assert HydrologyGraphsApp._should_use_palette_row(compact_row) is False


def test_graph_style_fields_y2_label_is_hyetograph_only():
    app = HydrologyGraphsApp.__new__(HydrologyGraphsApp)
    base_style = {
        "x_axis": {"tick_interval_hours": 6},
        "threshold": {"label_enabled": True, "label_offset": 0.0},
    }

    hyeto_fields = app._graph_style_fields_for("hyetograph:3day", dict(base_style))
    hydro_fields = app._graph_style_fields_for("hydrograph_discharge:3day", dict(base_style))

    hyeto_paths = {str(field.get("path", "")) for field in hyeto_fields}
    hydro_paths = {str(field.get("path", "")) for field in hydro_fields}
    assert "y2_axis.label" in hyeto_paths
    assert "y2_axis.label_rotation" in hyeto_paths
    assert "y2_axis.label" not in hydro_paths
    assert "y2_axis.label_rotation" not in hydro_paths


def test_graph_style_fields_hides_unused_x_axis_fields_for_annual():
    app = HydrologyGraphsApp.__new__(HydrologyGraphsApp)
    annual_style = {
        "x_axis": {
            "tick_hours_of_day": "1,2,3,4",
            "tick_interval_hours": 6,
            "year_tick_step": 2,
            "show_date_labels": True,
            "date_boundary_line_enabled": True,
            "date_boundary_line_offset_hours": 0.0,
            "data_trim_enabled": True,
            "data_trim_start_hours": 0.0,
            "data_trim_end_hours": 0.0,
        },
        "threshold": {"label_enabled": True, "label_offset": 0.0},
    }

    fields = app._graph_style_fields_for("annual_max_rainfall", annual_style)
    paths = {str(field.get("path", "")) for field in fields}

    assert "x_axis.tick_hours_of_day" not in paths
    assert "x_axis.tick_interval_hours" not in paths
    assert "x_axis.year_tick_step" in paths
    assert "x_axis.show_date_labels" not in paths
    assert "x_axis.date_boundary_line_enabled" not in paths
    assert "x_axis.date_boundary_line_offset_hours" not in paths
    assert "x_axis.data_trim_enabled" not in paths
    assert "x_axis.data_trim_start_hours" not in paths
    assert "x_axis.data_trim_end_hours" not in paths
