from __future__ import annotations

from datetime import date

from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.services.dto import PrecheckItem
from hydrology_graphs.ui.view_models import (
    build_batch_targets,
    build_preview_choices,
    graph_targets_from_precheck_items,
    parse_base_dates_text,
    selected_station_pairs,
)


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
