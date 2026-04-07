from __future__ import annotations

from datetime import datetime

import matplotlib.dates as mdates

from hydrology_graphs.render.plotter import _format_24h_tick


def test_format_24h_tick_turns_midnight_into_previous_day_24h():
    midnight = mdates.date2num(datetime(2025, 1, 2, 0, 0))
    one_am = mdates.date2num(datetime(2025, 1, 2, 1, 0))

    assert _format_24h_tick(midnight) == "01/01 24"
    assert _format_24h_tick(one_am) == "01/02 01"
