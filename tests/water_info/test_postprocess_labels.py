from pathlib import Path

import pandas as pd

from src.water_info.postprocess import _build_arg_parser
from src.water_info.postprocess import main as postprocess_main
from src.water_info.postprocess_labels import (
    build_output_label_maps,
    detect_metric_from_path,
)
from src.water_info.postprocess_service import PostprocessRequest, run_postprocess


def test_water_level_output_labels_are_dynamic():
    main_map, peak_map, year_map = build_output_label_maps("water_level", include_daily=True)

    assert main_map["hourly_daily_avg_var_den"] == "日平均水位（可変分母）"
    assert peak_map["peak_max_value"] == "最高水位"
    assert year_map["ikyo_drought_var_den"] == "位況渇水位（可変分母）"


def test_discharge_output_labels_are_dynamic():
    main_map, peak_map, year_map = build_output_label_maps("discharge", include_daily=False)

    assert main_map["hourly_daily_avg_var_den"] == "日平均流量（可変分母）"
    assert "daily_value" not in main_map
    assert peak_map["peak_max_time"] == "最大流量発生日時（水文水質DB基準）"
    assert year_map["ikyo_drought_fixed_den"] == "流況渇水流量（固定分母）"


def test_metric_is_suggested_from_water_info_file_suffix():
    assert detect_metric_from_path("303_station_2024_WH.xlsx") == "water_level"
    assert detect_metric_from_path(Path("303_station_2024_QH.xlsx")) == "discharge"
    assert detect_metric_from_path("303_station_2024_RH.xlsx") is None


def test_postprocess_cli_metric_defaults_to_water_level():
    parser = _build_arg_parser()

    assert parser.parse_args([]).metric == "water_level"
    assert parser.parse_args(["--metric", "discharge"]).metric == "discharge"


def test_postprocess_service_passes_metric_to_core(monkeypatch, tmp_path):
    called: list[list[str]] = []

    def fake_main(argv):
        called.append(argv)
        return 0

    monkeypatch.setattr("src.water_info.postprocess_service.postprocess_main", fake_main)
    request = PostprocessRequest(
        hour_file=tmp_path / "hour_QH.xlsx",
        daily_file=None,
        out_excel=tmp_path / "result.xlsx",
        out_parquet=tmp_path / "parquet",
        metric="discharge",
    )

    result = run_postprocess(request)

    assert result.metric == "discharge"
    assert called == [
        [
            "--hour-file",
            str(tmp_path / "hour_QH.xlsx"),
            "--out-excel",
            str(tmp_path / "result.xlsx"),
            "--metric",
            "discharge",
            "--out-parquet",
            str(tmp_path / "parquet"),
        ]
    ]


def test_postprocess_writes_metric_specific_excel_labels(tmp_path):
    hour_file = tmp_path / "station_QH.xlsx"
    output_file = tmp_path / "result.xlsx"
    dates = pd.date_range("2024-01-01 01:00", periods=48, freq="h")
    pd.DataFrame({"period_end_at": dates, "value": range(48)}).to_excel(
        hour_file,
        index=False,
        sheet_name="全期間",
    )

    assert postprocess_main(
        [
            "--hour-file",
            str(hour_file),
            "--out-excel",
            str(output_file),
            "--metric",
            "discharge",
        ]
    ) == 0

    peaks = pd.read_excel(output_file, sheet_name="peaks")
    summary = pd.read_excel(output_file, sheet_name="summary_adj")

    assert "最大流量" in peaks.columns
    assert "最大流量発生日時（水文水質DB基準）" in peaks.columns
    assert "流況豊水流量（可変分母）" in set(summary["項目"].astype(str))


def test_postprocess_app_is_registered():
    from src.water_info_acquirer.app_meta import get_module_title
    from src.water_info_acquirer.app_registry import APP_DEFINITION_BY_KEY

    assert get_module_title("postprocess") == "水文統計（位況・流況）"
    assert APP_DEFINITION_BY_KEY["postprocess"].description
