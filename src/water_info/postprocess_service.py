"""GUI/CLI共通の後処理実行境界。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .postprocess import main as postprocess_main
from .postprocess_labels import MetricKey, normalize_metric


@dataclass(frozen=True, slots=True)
class PostprocessRequest:
    """後処理に必要なGUI入力。"""

    hour_file: Path
    daily_file: Path | None
    out_excel: Path
    out_parquet: Path | None
    metric: MetricKey


@dataclass(frozen=True, slots=True)
class PostprocessResult:
    """後処理完了時にGUIへ返す結果。"""

    out_excel: Path
    out_parquet: Path | None
    metric: MetricKey


def run_postprocess(request: PostprocessRequest) -> PostprocessResult:
    """CLIと同じ後処理をGUIから実行する。"""
    metric = normalize_metric(request.metric)
    argv = [
        "--hour-file",
        str(request.hour_file),
        "--out-excel",
        str(request.out_excel),
        "--metric",
        metric,
    ]
    if request.daily_file is not None:
        argv.extend(("--daily-file", str(request.daily_file)))
    if request.out_parquet is not None:
        argv.extend(("--out-parquet", str(request.out_parquet)))

    postprocess_main(argv)
    return PostprocessResult(
        out_excel=request.out_excel,
        out_parquet=request.out_parquet,
        metric=metric,
    )
