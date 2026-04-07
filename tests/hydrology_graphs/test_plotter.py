from __future__ import annotations

from datetime import datetime

import matplotlib.dates as mdates
import pandas as pd

from hydrology_graphs.render.plotter import _format_24h_tick
from hydrology_graphs.render.plotter import _time_column_for_plot


def test_format_24h_tick_turns_midnight_into_previous_day_24h():
    midnight = mdates.date2num(datetime(2025, 1, 2, 0, 0))
    one_am = mdates.date2num(datetime(2025, 1, 2, 1, 0))

    assert _format_24h_tick(midnight) == "01/01 24"
    assert _format_24h_tick(one_am) == "01/02 01"


def test_time_column_for_plot_falls_back_to_observed_at_when_period_end_all_missing():
    df = pd.DataFrame(
        {
            "period_end_at": [None, None],
            "observed_at": ["2025-01-01 00:00:00", "2025-01-01 01:00:00"],
        }
    )
    assert _time_column_for_plot(df) == "observed_at"


def test_time_column_for_plot_uses_period_end_at_when_present():
    df = pd.DataFrame(
        {
            "period_end_at": ["2025-01-01 00:00:00", None],
            "observed_at": ["2025-01-01 00:00:00", "2025-01-01 01:00:00"],
        }
    )
    assert _time_column_for_plot(df) == "period_end_at"
