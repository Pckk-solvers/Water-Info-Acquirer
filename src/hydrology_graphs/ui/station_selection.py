from __future__ import annotations

import tkinter.font as tkfont
from typing import Any

from .view_models import format_station_display_text


def station_display_text(app, source: str, station_key: str, station_name: str, checked: bool) -> str:
    metric_labels = app._station_metric_labels.get((source, station_key), ())
    return format_station_display_text(
        source=source,
        station_key=station_key,
        station_name=station_name,
        checked=checked,
        source_label_map=app.SOURCE_LABELS,
        metric_labels=metric_labels,
    )


def render_station_check_list(app) -> None:
    """観測所一覧をチェック表現で再描画する。"""

    yview = app.station_list.yview()
    app.station_list.delete(0, "end")
    app._station_row_pairs = []
    total = len(app._catalog_stations)
    for idx, (source, station_key, station_name) in enumerate(app._catalog_stations):
        pair = (source, station_key)
        checked = pair in app._checked_station_pairs
        app.station_list.insert("end", station_display_text(app, source, station_key, station_name, checked))
        app._station_row_pairs.append(pair)
        if idx < total - 1:
            app.station_list.insert("end", " ")
            app._station_row_pairs.append(None)
    app.station_list.selection_clear(0, "end")
    if yview:
        app.station_list.yview_moveto(float(yview[0]))


def update_station_row_display(app, index: int) -> None:
    if index < 0 or index >= len(app._station_row_pairs):
        return
    pair = app._station_row_pairs[index]
    if pair is None:
        return
    source, station_key = pair
    station_name = ""
    for s, k, name in app._catalog_stations:
        if s == source and k == station_key:
            station_name = name
            break
    checked = pair in app._checked_station_pairs
    app.station_list.delete(index)
    app.station_list.insert(index, station_display_text(app, source, station_key, station_name, checked))


def selected_station_pairs_in_order(app) -> list[tuple[str, str]]:
    return [pair for pair in app._station_row_pairs if pair is not None and pair in app._checked_station_pairs]


def toggle_station_at_index(app, index: int) -> None:
    if index < 0 or index >= len(app._station_row_pairs):
        return
    pair = app._station_row_pairs[index]
    if pair is None:
        return
    if pair in app._checked_station_pairs:
        app._checked_station_pairs.remove(pair)
    else:
        app._checked_station_pairs.add(pair)
    app._station_selection_dirty = True
    update_station_row_display(app, index)


def station_checkbox_hit_width(app) -> int:
    try:
        font = tkfont.nametofont(str(app.station_list.cget("font")))
        return max(18, int(font.measure("☐ ")) + 6)
    except Exception:  # noqa: BLE001
        return 24


def on_station_list_click(app, event: Any) -> str:
    try:
        index = int(app.station_list.nearest(event.y))
    except Exception:  # noqa: BLE001
        return "break"
    bbox = app.station_list.bbox(index)
    if not bbox:
        return "break"
    pair = app._station_row_pairs[index] if 0 <= index < len(app._station_row_pairs) else None
    if pair is None:
        return "break"
    x, _y, _w, _h = bbox
    if int(getattr(event, "x", 0)) > x + station_checkbox_hit_width(app):
        return "break"
    toggle_station_at_index(app, index)
    return "break"


def select_all_stations(app) -> None:
    app._checked_station_pairs = {(source, key) for source, key, _name in app._catalog_stations}
    app._station_selection_dirty = True
    render_station_check_list(app)


def clear_all_stations(app) -> None:
    app._checked_station_pairs.clear()
    app._station_selection_dirty = True
    render_station_check_list(app)


def apply_station_checks(app) -> None:
    selected_pairs = selected_station_pairs_in_order(app)
    if selected_pairs:
        if not app._ensure_full_catalog_loaded():
            return
    app._recompute_base_date_candidates_for_selected_stations()
    app._station_selection_dirty = False
    app._append_log(f"[STATION] apply checks selected={len(selected_pairs)} dates={len(app._base_dates)}")
