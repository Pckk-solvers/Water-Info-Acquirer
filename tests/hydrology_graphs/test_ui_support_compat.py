from __future__ import annotations

from hydrology_graphs.services import ui_support


def test_services_ui_support_reexports_symbols():
    assert hasattr(ui_support, "build_preview_choices")
    assert hasattr(ui_support, "selected_station_pairs")
