from .renderer_csv import render_station_csv_row, write_station_csv
from .renderer_markdown import render_markdown
from .service import scrape_station
from .rainfall.services import RainfallAnalyzeResult, RainfallRunInput, run_rainfall_analyze, run_rainfall_collect


def main(argv=None):
    from .cli import main as _main

    return _main(argv)

__all__ = [
    "main",
    "scrape_station",
    "render_markdown",
    "render_station_csv_row",
    "write_station_csv",
    "RainfallAnalyzeResult",
    "RainfallRunInput",
    "run_rainfall_analyze",
    "run_rainfall_collect",
]
