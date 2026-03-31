from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

"""サービス層の入出力 DTO 定義。

UI からサービスへ渡す入力と、結果表示に使う出力を分離する。
"""


@dataclass(frozen=True, slots=True)
class PrecheckInput:
    """実行前検証に必要な入力。"""

    parquet_dir: str
    threshold_file_path: str | None
    graph_types: list[str]
    base_dates: list[str]
    event_window_days: int = 3
    station_pairs: list[tuple[str, str]] | None = None
    station_keys: list[str] = field(default_factory=list)
    sources: list[str] | None = None


@dataclass(frozen=True, slots=True)
class PreviewInput:
    """プレビュー描画に必要な入力。"""

    parquet_dir: str
    threshold_file_path: str | None
    style_json_path: str | None
    style_payload: dict | None
    source: str
    station_key: str
    graph_type: str
    base_datetime: str | None
    event_window_days: int | None


@dataclass(frozen=True, slots=True)
class BatchTarget:
    """バッチ 1 件分の対象。"""

    source: str
    station_key: str
    graph_type: str
    base_datetime: str | None
    event_window_days: int | None


@dataclass(frozen=True, slots=True)
class BatchRunInput:
    """バッチ実行に必要な入力。"""

    parquet_dir: str
    output_dir: str
    threshold_file_path: str | None
    style_json_path: str | None
    style_payload: dict | None
    targets: list[BatchTarget]
    should_stop: Callable[[], bool] | None = None


@dataclass(frozen=True, slots=True)
class PrecheckSummary:
    """実行前検証の集計結果。"""

    total_targets: int
    ok_targets: int
    ng_targets: int


@dataclass(frozen=True, slots=True)
class PrecheckItem:
    """実行前検証の 1 件分結果。"""

    target_id: str
    source: str
    station_key: str
    graph_type: str
    base_datetime: str | None
    status: str
    reason_code: str | None = None
    reason_message: str | None = None


@dataclass(slots=True)
class PrecheckResult:
    """実行前検証の全体結果。"""

    summary: PrecheckSummary
    items: list[PrecheckItem] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PreviewResult:
    """プレビュー描画の結果。"""

    status: str
    reason_code: str | None
    reason_message: str | None
    image_bytes_png: bytes | None


@dataclass(frozen=True, slots=True)
class BatchRunItemResult:
    """バッチ 1 件分の実行結果。"""

    target_id: str
    status: str
    reason_code: str | None = None
    reason_message: str | None = None
    output_path: str | None = None


@dataclass(frozen=True, slots=True)
class BatchSummary:
    """バッチ実行全体の集計。"""

    total: int
    success: int
    failed: int
    skipped: int


@dataclass(slots=True)
class BatchRunResult:
    """バッチ実行の全体結果。"""

    summary: BatchSummary
    items: list[BatchRunItemResult] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)
