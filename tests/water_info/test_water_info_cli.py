from __future__ import annotations

from pathlib import Path

from src.water_info import cli
from src.water_info.entry import WaterInfoOutputResult


def test_parse_period_from_year_month() -> None:
    period = cli._parse_period("2024-01", "2024-12")
    assert period.year_start == "2024"
    assert period.year_end == "2024"
    assert period.month_start == "1月"
    assert period.month_end == "12月"


def test_fetch_returns_partial_exit_code(monkeypatch) -> None:
    def _fake_run(*, code, request, output_dir):
        if code == "200":
            raise RuntimeError("boom")
        return WaterInfoOutputResult(
            code=code,
            station_name="観測所",
            excel_path=Path("out.xlsx"),
            parquet_path=None,
            unified_records=(),
        )

    monkeypatch.setattr(cli, "run_cli_request_for_code", _fake_run)

    code = cli.main(
        [
            "fetch",
            "--code",
            "100",
            "--code",
            "200",
            "--mode",
            "S",
            "--start",
            "2024-01",
            "--end",
            "2024-02",
            "--interval",
            "hourly",
        ]
    )

    assert code == 2


def test_fetch_passes_custom_output_dir(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(*, code, request, output_dir):
        captured["code"] = code
        captured["output_dir"] = output_dir
        captured["export_excel"] = request.options.export_excel
        captured["export_ndjson"] = request.options.export_ndjson
        return WaterInfoOutputResult(
            code=code,
            station_name="観測所",
            excel_path=Path("out.xlsx"),
            parquet_path=Path("out.parquet"),
            unified_records=(),
        )

    monkeypatch.setattr(cli, "run_cli_request_for_code", _fake_run)

    code = cli.main(
        [
            "fetch",
            "--code",
            "303031283302005",
            "--mode",
            "U",
            "--start",
            "2024-01",
            "--end",
            "2024-03",
            "--interval",
            "daily",
            "--ndjson",
            "--no-excel",
            "--parquet",
            "--output-dir",
            "custom-water",
        ]
    )

    assert code == 0
    assert captured["code"] == "303031283302005"
    assert captured["output_dir"] == Path("custom-water")
    assert captured["export_excel"] is False
    assert captured["export_ndjson"] is True


def test_fetch_writes_single_combined_ndjson(monkeypatch, tmp_path) -> None:
    emitted_paths: list[Path] = []

    def _fake_run(*, code, request, output_dir):
        return WaterInfoOutputResult(
            code=code,
            station_name=f"観測所-{code}",
            excel_path=None,
            parquet_path=None,
            unified_records=(
                {
                    "source": "water_info",
                    "station_key": code,
                    "station_name": f"観測所-{code}",
                    "period_start_at": None,
                    "period_end_at": None,
                    "observed_at": "2024-01-01T00:00:00",
                    "metric": "water_level",
                    "value": 1.23,
                    "unit": "m",
                    "interval": "1hour",
                    "quality": "normal",
                },
            ),
        )

    def _fake_save(records, output_path):
        emitted_paths.append(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text("ok", encoding="utf-8")
        return output_path

    monkeypatch.setattr(cli, "run_cli_request_for_code", _fake_run)
    monkeypatch.setattr(cli, "save_unified_records_ndjson", _fake_save)

    code = cli.main(
        [
            "fetch",
            "--code",
            "100",
            "--code",
            "200",
            "--mode",
            "S",
            "--start",
            "2024-01",
            "--end",
            "2024-02",
            "--interval",
            "hourly",
            "--ndjson",
            "--output-dir",
            str(tmp_path),
        ]
    )

    assert code == 0
    assert len(emitted_paths) == 1
    assert emitted_paths[0].name == "water_info_batch_water_level_1hour_202401_202402.ndjson"
