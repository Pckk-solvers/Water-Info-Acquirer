from __future__ import annotations

from pathlib import Path

from src.jma_rainfall_pipeline import cli
from src.jma_rainfall_pipeline.controller.weather_data_controller import (
    StationExportResult,
    WeatherExportSummary,
)


def test_parse_station_token_normalizes_obs_type() -> None:
    assert cli._parse_station_token("11:47401:a") == ("11", "47401", "a1")
    assert cli._parse_station_token("11:47401:s1") == ("11", "47401", "s1")


def test_run_list_stations_accepts_prefecture_name(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli, "fetch_prefecture_codes", lambda: [("11", "埼玉県")])
    monkeypatch.setattr(
        cli,
        "fetch_station_codes",
        lambda pref_code: [{"block_no": "47401", "station": "田無", "obs_method": "a"}],
    )

    code = cli.main(["list-stations", "--pref", "埼玉県", "--format", "json"])

    assert code == 0
    assert '"pref_code": "11"' in capsys.readouterr().out


def test_fetch_uses_custom_output_root(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeController:
        def __init__(self, *, interval):
            captured["interval"] = interval

        def fetch_and_export_summary(self, **kwargs):
            captured.update(kwargs)
            return WeatherExportSummary(
                results=(
                    StationExportResult(
                        prec_no="11",
                        block_no="47401",
                        interval_label="hourly",
                        csv_path=Path("out.csv"),
                        excel_path=Path("out.xlsx"),
                        parquet_path=Path("out.parquet"),
                        request_urls=(),
                    ),
                )
            )

    monkeypatch.setattr(cli, "WeatherDataController", _FakeController)
    monkeypatch.setattr(cli, "setup_logging", lambda **kwargs: None)
    monkeypatch.setattr(cli, "set_runtime_log_options", lambda **kwargs: None)

    code = cli.main(
        [
            "fetch",
            "--station",
            "11:47401:a",
            "--start",
            "2025-01-01",
            "--end",
            "2025-01-31",
            "--interval",
            "hourly",
            "--csv",
            "--excel",
            "--parquet",
            "--output-dir",
            "custom-root",
        ]
    )

    assert code == 0
    assert captured["output_dir"] == Path("custom-root")
    assert captured["stations"] == [("11", "47401", "a1")]
