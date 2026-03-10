from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall import cli
from river_meta.rainfall.domain.models import RainfallDataset
from river_meta.rainfall.services import RainfallAnalyzeResult


def test_rainfall_cli_collection_order_default_station_year(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeRunInput:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            for key, value in kwargs.items():
                setattr(self, key, value)

    monkeypatch.setattr("river_meta.rainfall.cli.RainfallRunInput", _FakeRunInput)

    args = cli.build_parser().parse_args(
        [
            "--source",
            "water_info",
            "--year",
            "2025",
            "--output-dir",
            "outputs/river_meta/rainfall",
        ]
    )

    config = cli._build_run_input(args)
    assert captured["collection_order"] == "station_year"
    assert config.collection_order == "station_year"


def test_rainfall_cli_collection_order_explicit_year_station(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeRunInput:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            for key, value in kwargs.items():
                setattr(self, key, value)

    monkeypatch.setattr("river_meta.rainfall.cli.RainfallRunInput", _FakeRunInput)

    args = cli.build_parser().parse_args(
        [
            "--source",
            "water_info",
            "--year",
            "2025",
            "--collection-order",
            "year_station",
            "--output-dir",
            "outputs/river_meta/rainfall",
        ]
    )

    config = cli._build_run_input(args)
    assert captured["collection_order"] == "year_station"
    assert config.collection_order == "year_station"


def test_rainfall_cli_collection_order_compatible_with_existing_args(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeRunInput:
        def __init__(self, **kwargs):
            for key, value in kwargs.items():
                setattr(self, key, value)

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
        return RainfallAnalyzeResult(
            dataset=RainfallDataset(records=[], errors=[]),
            timeseries_df=pd.DataFrame(),
            annual_max_df=pd.DataFrame(),
            excel_paths=[],
            chart_paths=[],
        )

    monkeypatch.setattr("river_meta.rainfall.cli.RainfallRunInput", _FakeRunInput)
    monkeypatch.setattr("river_meta.rainfall.cli.run_rainfall_analyze", _fake_analyze)

    code = cli.main(
        [
            "--source",
            "water_info",
            "--year",
            "2025",
            "--interval",
            "10min",
            "--include-raw",
            "--waterinfo-pref-list",
            "大阪,京都",
            "--collection-order",
            "year_station",
            "--output-dir",
            "outputs/river_meta/rainfall",
        ]
    )

    assert code == 0
    config = captured["config"]
    assert config.year == 2025
    assert config.interval == "10min"
    assert config.include_raw is True
    assert config.waterinfo_prefectures == ["大阪", "京都"]
    assert config.collection_order == "year_station"


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


def test_rainfall_cli_generate_default_diff_mode_enabled(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeGenerateInput:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _fake_generate(config, *, log, should_stop):
        captured["config"] = config
        return SimpleNamespace(entries=[], incomplete_entries=[], excel_paths=[], chart_paths=[], errors=[])

    monkeypatch.setattr("river_meta.rainfall.cli.RainfallGenerateInput", _FakeGenerateInput)
    monkeypatch.setattr("river_meta.rainfall.cli.run_rainfall_generate", _fake_generate)

    code = cli.main(
        [
            "--mode",
            "generate",
            "--year",
            "2025",
            "--output-dir",
            "outputs/river_meta/rainfall",
        ]
    )

    assert code == 0
    kwargs = captured["kwargs"]
    assert kwargs["use_diff_mode"] is True
    assert kwargs["force_full_regenerate"] is False


def test_rainfall_cli_generate_force_full_regenerate_has_priority(monkeypatch):
    captured: dict[str, object] = {}

    class _FakeGenerateInput:
        def __init__(self, **kwargs):
            captured["kwargs"] = kwargs
            for key, value in kwargs.items():
                setattr(self, key, value)

    def _fake_generate(config, *, log, should_stop):
        return SimpleNamespace(entries=[], incomplete_entries=[], excel_paths=[], chart_paths=[], errors=[])

    monkeypatch.setattr("river_meta.rainfall.cli.RainfallGenerateInput", _FakeGenerateInput)
    monkeypatch.setattr("river_meta.rainfall.cli.run_rainfall_generate", _fake_generate)

    code = cli.main(
        [
            "--mode",
            "generate",
            "--year",
            "2025",
            "--output-dir",
            "outputs/river_meta/rainfall",
            "--use-diff-mode",
            "--force-full-regenerate",
        ]
    )

    assert code == 0
    kwargs = captured["kwargs"]
    assert kwargs["force_full_regenerate"] is True
    assert kwargs["use_diff_mode"] is False
