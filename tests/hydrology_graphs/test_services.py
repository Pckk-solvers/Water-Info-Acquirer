from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from hydrology_graphs.io.style_store import default_style
from hydrology_graphs.services.dto import BatchRunInput, BatchTarget, PrecheckInput, PreviewInput
from hydrology_graphs.services.usecases import (
    HydrologyGraphService,
    precheck_graph_targets,
    preview_graph_target,
    run_graph_batch,
)


def _write_timeseries(parquet_dir, *, station_key="111") -> None:
    rows = []
    start = datetime(2025, 1, 1)
    for i in range(72):
        rows.append(
            {
                "source": "jma",
                "station_key": station_key,
                "station_name": "A",
                "observed_at": start + timedelta(hours=i),
                "metric": "rainfall",
                "value": float(i % 5 + 1),
                "unit": "mm",
                "interval": "1hour",
                "quality": "normal",
            }
        )
    for year in range(2010, 2020):
        rows.append(
            {
                "source": "jma",
                "station_key": station_key,
                "station_name": "A",
                "observed_at": datetime(year, 1, 1),
                "metric": "rainfall",
                "value": float(year - 2000),
                "unit": "mm",
                "interval": "1hour",
                "quality": "normal",
            }
        )
    pd.DataFrame(rows).to_parquet(parquet_dir / "timeseries.parquet", index=False)


def _write_thresholds(path, *, station_key="111") -> None:
    path.write_text(
        "\n".join(
            [
                "source,station_key,graph_type,line_name,value,unit,line_color,line_style,line_width,priority,enabled",
                f"jma,{station_key},hyetograph,基準,10,mm,#111111,solid,1.0,1,true",
            ]
        ),
        encoding="utf-8",
    )


def test_preview_graph_target_returns_png_bytes(tmp_path):
    _write_timeseries(tmp_path)
    _write_thresholds(tmp_path / "thresholds.csv")

    result = preview_graph_target(
        PreviewInput(
            parquet_dir=str(tmp_path),
            threshold_file_path=str(tmp_path / "thresholds.csv"),
            style_json_path=None,
            style_payload=default_style(),
            source="jma",
            station_key="111",
            graph_type="hyetograph",
            base_datetime="2025-01-02",
            event_window_days=3,
        )
    )

    assert result.status == "success"
    assert result.image_bytes_png[:8] == b"\x89PNG\r\n\x1a\n"


def test_preview_graph_target_returns_threshold_not_found(tmp_path):
    _write_timeseries(tmp_path)
    other = tmp_path / "thresholds.csv"
    other.write_text(
        "source,station_key,graph_type,line_name,value,unit\n"
        "jma,999,hyetograph,基準,10,mm\n",
        encoding="utf-8",
    )

    result = preview_graph_target(
        PreviewInput(
            parquet_dir=str(tmp_path),
            threshold_file_path=str(other),
            style_json_path=None,
            style_payload=default_style(),
            source="jma",
            station_key="111",
            graph_type="hyetograph",
            base_datetime="2025-01-02",
            event_window_days=3,
        )
    )

    assert result.status == "error"
    assert result.reason_code == "threshold_not_found"


def test_preview_graph_target_returns_style_error(tmp_path):
    _write_timeseries(tmp_path)

    payload = default_style()
    payload["schema_version"] = "0.9"
    result = preview_graph_target(
        PreviewInput(
            parquet_dir=str(tmp_path),
            threshold_file_path=None,
            style_json_path=None,
            style_payload=payload,
            source="jma",
            station_key="111",
            graph_type="hyetograph",
            base_datetime="2025-01-02",
            event_window_days=3,
        )
    )

    assert result.status == "error"
    assert result.reason_code == "style_error"


def test_precheck_reports_insufficient_years(tmp_path):
    frame = pd.DataFrame(
        {
            "source": ["jma"],
            "station_key": ["111"],
            "station_name": ["A"],
            "observed_at": [datetime(2025, 1, 1)],
            "metric": ["rainfall"],
            "value": [1.0],
            "unit": ["mm"],
            "interval": ["1hour"],
            "quality": ["normal"],
        }
    )
    frame.to_parquet(tmp_path / "one_year.parquet", index=False)

    result = precheck_graph_targets(
        PrecheckInput(
            parquet_dir=str(tmp_path),
            threshold_file_path=None,
            graph_types=["annual_max_rainfall"],
            station_keys=["111"],
            base_dates=[],
            event_window_days=3,
            sources=["jma"],
        )
    )

    assert result.summary.ng_targets == 1
    assert result.items[0].reason_code == "insufficient_years"


def test_precheck_uses_station_pairs_without_cross_product(tmp_path):
    rows = []
    start = datetime(2025, 1, 1)
    for i in range(72):
        rows.append(
            {
                "source": "jma",
                "station_key": "111",
                "station_name": "A",
                "observed_at": start + timedelta(hours=i),
                "metric": "rainfall",
                "value": float(i % 4 + 1),
                "unit": "mm",
                "interval": "1hour",
                "quality": "normal",
            }
        )
        rows.append(
            {
                "source": "water_info",
                "station_key": "222",
                "station_name": "B",
                "observed_at": start + timedelta(hours=i),
                "metric": "rainfall",
                "value": float(i % 4 + 1),
                "unit": "mm",
                "interval": "1hour",
                "quality": "normal",
            }
        )
    pd.DataFrame(rows).to_parquet(tmp_path / "pairs.parquet", index=False)

    result = precheck_graph_targets(
        PrecheckInput(
            parquet_dir=str(tmp_path),
            threshold_file_path=None,
            graph_types=["hyetograph"],
            station_pairs=[("jma", "111"), ("water_info", "222")],
            base_dates=["2025-01-02"],
            event_window_days=3,
        )
    )

    assert result.summary.total_targets == 2
    assert result.summary.ok_targets == 2
    assert result.summary.ng_targets == 0
    assert sorted((item.source, item.station_key) for item in result.items) == [("jma", "111"), ("water_info", "222")]


def test_run_graph_batch_writes_png(tmp_path):
    _write_timeseries(tmp_path)
    threshold_path = tmp_path / "thresholds.csv"
    _write_thresholds(threshold_path)
    output_dir = tmp_path / "out"

    result = run_graph_batch(
        BatchRunInput(
            parquet_dir=str(tmp_path),
            output_dir=str(output_dir),
            threshold_file_path=str(threshold_path),
            style_json_path=None,
            style_payload=default_style(),
            targets=[
                BatchTarget(
                    source="jma",
                    station_key="111",
                    graph_type="hyetograph",
                    base_datetime="2025-01-02",
                    event_window_days=3,
                )
            ],
        )
    )

    assert result.summary.success == 1
    assert result.items[0].status == "success"
    assert result.items[0].output_path is not None
    assert (output_dir / "111" / "hyetograph" / "2025-01-02" / "graph.png").exists()
