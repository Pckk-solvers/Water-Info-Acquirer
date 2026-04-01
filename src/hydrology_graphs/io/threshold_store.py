from __future__ import annotations

import csv
import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..domain.constants import GRAPH_TYPES
from ..domain.logic import threshold_key
from ..domain.models import ThresholdRecord

"""基準線定義の読み込みと正規化。

CSV / JSON の両方に対応し、優先順位や有効/無効もこの層で整理する。
"""


@dataclass(slots=True)
class ThresholdLoadResult:
    """読み込み後の基準線一覧と警告をまとめる。"""

    lines: list[ThresholdRecord]
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class ThresholdCacheState:
    """しきい値読込結果のキャッシュ状態。"""

    path: str | None = None
    mtime_ns: int | None = None
    result: ThresholdLoadResult | None = None


def load_thresholds(path: str | Path | None) -> ThresholdLoadResult:
    """基準線ファイルを読み込んで正規化する。"""

    if path is None or not str(path).strip():
        return ThresholdLoadResult(lines=[], warnings=[])

    file_path = Path(path)
    if not file_path.exists():
        return ThresholdLoadResult(lines=[], warnings=[f"threshold_file_not_found: {file_path}"])

    suffix = file_path.suffix.lower()
    try:
        if suffix == ".csv":
            rows = _load_csv_rows(file_path)
        elif suffix == ".json":
            rows = _load_json_rows(file_path)
        else:
            return ThresholdLoadResult(lines=[], warnings=[f"unsupported_threshold_ext: {suffix}"])
    except Exception as exc:  # noqa: BLE001
        return ThresholdLoadResult(lines=[], warnings=[f"threshold_read_error: {type(exc).__name__}: {exc}"])

    warnings: list[str] = []
    parsed: list[tuple[int, ThresholdRecord]] = []
    for idx, row in enumerate(rows):
        # 1行ずつ検証して、壊れた行だけを落とす。
        line, error = _parse_row(row, order_index=idx)
        if error is not None:
            warnings.append(f"row_{idx + 1}: {error}")
            continue
        if line is not None:
            parsed.append((idx, line))

    if not parsed and rows:
        warnings.append("threshold_all_rows_invalid")
    lines = _resolve_priority(parsed)
    return ThresholdLoadResult(lines=lines, warnings=warnings)


def load_thresholds_with_cache(
    path: str | Path | None,
    *,
    cache: ThresholdCacheState,
) -> ThresholdLoadResult | None:
    """しきい値ファイルをキャッシュ利用で読み込む。"""

    if path is None or not str(path).strip():
        return None
    file_path = Path(path)
    try:
        mtime_ns = file_path.stat().st_mtime_ns
    except OSError:
        return load_thresholds(path)
    if cache.path == str(file_path) and cache.mtime_ns == mtime_ns and cache.result is not None:
        return cache.result
    result = load_thresholds(path)
    cache.path = str(file_path)
    cache.mtime_ns = mtime_ns
    cache.result = result
    return result


def group_thresholds(lines: list[ThresholdRecord]) -> dict[str, list[ThresholdRecord]]:
    """検索キーごとに基準線をグルーピングする。"""

    grouped: dict[str, list[ThresholdRecord]] = {}
    for line in lines:
        key = threshold_key(line.source, line.station_key, line.graph_type)
        grouped.setdefault(key, []).append(line)
    return grouped


def thresholds_for_key(
    lines: list[ThresholdRecord],
    *,
    source: str,
    station_key: str,
    graph_type: str,
) -> list[ThresholdRecord]:
    """指定キーに一致する基準線だけを返す。"""

    key = threshold_key(source, station_key, graph_type)
    return group_thresholds(lines).get(key, [])


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    """CSV の各行を辞書として読む。"""

    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        return [dict(row) for row in reader]


def _load_json_rows(path: Path) -> list[dict[str, Any]]:
    """JSON 配列を行データへ変換する。"""

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("root must be an array")
    rows: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("threshold items must be objects")
        rows.append(dict(item))
    return rows


def _parse_row(row: dict[str, Any], *, order_index: int) -> tuple[ThresholdRecord | None, str | None]:
    """1行分の基準線を検証して ThresholdRecord に変換する。"""

    required = ("source", "station_key", "graph_type", "line_name", "value", "unit")
    missing = [key for key in required if not str(row.get(key, "")).strip()]
    if missing:
        return None, f"missing_required: {', '.join(missing)}"

    source = str(row["source"]).strip()
    station_key = str(row["station_key"]).strip()
    graph_type = str(row["graph_type"]).strip()
    line_name = str(row["line_name"]).strip()
    unit = str(row["unit"]).strip()
    if not source or not station_key or not line_name:
        return None, "source/station_key/line_name must be non-empty"
    if graph_type not in GRAPH_TYPES:
        return None, f"invalid_graph_type: {graph_type}"

    try:
        value = float(row["value"])
    except Exception:  # noqa: BLE001
        return None, "value must be number"
    if not math.isfinite(value):
        return None, "value must be finite"

    enabled = _to_bool(row.get("enabled", True), default=True)
    if not enabled:
        return None, None

    priority = _to_int(row.get("priority", 0), default=0)
    line_width = _to_float(row.get("line_width", 1.2), default=1.2)
    if line_width <= 0:
        return None, "line_width must be > 0"
    line_style = str(row.get("line_style", "solid") or "solid").strip()
    if line_style not in ("solid", "dashed", "dotted"):
        return None, f"invalid_line_style: {line_style}"
    line_color = str(row.get("line_color", "")).strip() or None
    label = str(row.get("label", "")).strip() or None
    note = str(row.get("note", "")).strip() or None

    if line_color is not None and not _is_hex_color(line_color):
        return None, "line_color must be hex color"

    return (
        ThresholdRecord(
            source=source,  # type: ignore[arg-type]
            station_key=station_key,
            graph_type=graph_type,  # type: ignore[arg-type]
            line_name=line_name,
            value=value,
            unit=unit,
            line_color=line_color,
            line_style=line_style,
            line_width=line_width,
            label=label,
            priority=priority,
            enabled=True,
            note=note,
            order_index=order_index,
        ),
        None,
    )


def _resolve_priority(rows: list[tuple[int, ThresholdRecord]]) -> list[ThresholdRecord]:
    """priority の高い順に並べ、同率なら入力順を保つ。"""

    # priority が同じなら、もともとの並び順をそのまま残す。
    rows_sorted = sorted(rows, key=lambda item: (-item[1].priority, item[0]))
    return [line for _, line in rows_sorted]


def _to_bool(value: object, *, default: bool) -> bool:
    """文字列や数値を bool に寄せる。"""

    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _to_int(value: object, *, default: int) -> int:
    """安全に int へ変換する。"""

    try:
        return int(str(value).strip())
    except Exception:  # noqa: BLE001
        return default


def _to_float(value: object, *, default: float) -> float:
    """安全に float へ変換する。"""

    try:
        return float(str(value).strip())
    except Exception:  # noqa: BLE001
        return default


def _is_hex_color(value: str) -> bool:
    """16進カラー文字列かを判定する。"""

    if not value.startswith("#"):
        return False
    hex_part = value[1:]
    return len(hex_part) in (6, 8) and all(ch in "0123456789ABCDEFabcdef" for ch in hex_part)
