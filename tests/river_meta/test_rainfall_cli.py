from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall import cli
from river_meta.rainfall.models import RainfallDataset
from river_meta.services.rainfall import RainfallAnalyzeResult


def test_rainfall_cli_analyze_year_success(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_analyze(
        config,
        *,
        export_excel,
        export_chart,
        output_dir,
        decimal_places,
        log,
        should_stop,
    ):
        captured["config"] = config
        captured["export_excel"] = export_excel
        return RainfallAnalyzeResult(
            dataset=RainfallDataset(records=[], errors=[]),
            timeseries_df=pd.DataFrame(),
            annual_max_df=pd.DataFrame(),
            excel_paths=[],
            chart_paths=[],
        )

    monkeypatch.setattr("river_meta.rainfall.cli.run_rainfall_analyze", _fake_analyze)

    code = cli.main(
        [
            "--source",
            "water_info",
            "--year",
            "2025",
            "--output-dir",
            "outputs/river_meta/rainfall",
            "--waterinfo-pref-list",
            "大阪,京都",
        ]
    )
    assert code == 0
    config = captured["config"]
    assert config.year == 2025
    assert config.waterinfo_prefectures == ["大阪", "京都"]


def test_rainfall_cli_collect_cancelled_returns_130(monkeypatch):
    def _fake_collect(config, *, log, should_stop):
        return RainfallDataset(records=[], errors=["cancelled"])

    monkeypatch.setattr("river_meta.rainfall.cli.run_rainfall_collect", _fake_collect)

    code = cli.main(
        [
            "--mode",
            "collect",
            "--source",
            "water_info",
            "--year",
            "2025",
            "--output-dir",
            "outputs/river_meta/rainfall",
            "--waterinfo-station-code",
            "2700000001",
        ]
    )
    assert code == 130


def test_rainfall_cli_parse_date_end_of_day():
    parsed = cli._parse_datetime("2025-12-31", is_end=True)
    assert parsed == datetime(2025, 12, 31, 23, 59, 59)


def test_rainfall_cli_requires_output_dir():
    with pytest.raises(SystemExit):
        cli.main(["--source", "water_info", "--year", "2025"])
