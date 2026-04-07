from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.services.dto import BatchTarget, PrecheckItem


@dataclass(frozen=True, slots=True)
class PreviewChoices:
    """プレビュー候補の表示情報。"""

    station_display_to_pair: dict[str, tuple[str, str]]
    graph_display_to_key: dict[str, str]
    station_values: list[str]
    date_values: list[str]
    graph_values: list[str]


def parse_base_dates_text(text: str) -> list[str]:
    """改行区切りの基準日入力を正規化する。"""

    return [line.strip() for line in text.splitlines() if line.strip()]


def selected_event_windows(*, use_3day: bool, use_5day: bool) -> list[int]:
    """チェック状態からイベント窓リストを返す。"""

    result: list[int] = []
    if use_3day:
        result.append(3)
    if use_5day:
        result.append(5)
    return result


def selected_station_pairs(
    catalog_stations: list[tuple[str, str, str]],
    selected_indices: list[int],
) -> list[tuple[str, str]]:
    """Listbox 選択位置から重複なし station_pairs を作る。"""

    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for idx in selected_indices:
        source, station_key, _name = catalog_stations[int(idx)]
        pair = (source, station_key)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return pairs


def format_station_display_text(
    *,
    source: str,
    station_key: str,
    station_name: str,
    checked: bool,
    source_label_map: dict[str, str],
    metric_labels: tuple[str, ...] = (),
) -> str:
    """観測所一覧の表示テキストを作る。"""

    mark = "☑" if checked else "☐"
    source_label = source_label_map.get(source, source)
    suffix = f" ({station_name})" if station_name else ""
    metric_suffix = f" / {' / '.join(metric_labels)}" if metric_labels else ""
    return f"{mark} {source_label}:{station_key}{suffix}{metric_suffix}"


def graph_targets_from_precheck_items(
    *,
    items: list[PrecheckItem],
) -> list[GraphTarget]:
    """Precheck結果から OK 対象の GraphTarget 一覧を構築する。"""

    targets: list[GraphTarget] = []
    for row in items:
        if row.status != "ok":
            continue
        base = date.fromisoformat(row.base_datetime) if row.base_datetime else None
        targets.append(
            GraphTarget(
                source=row.source,
                station_key=row.station_key,
                graph_type=row.graph_type,  # type: ignore[arg-type]
                base_date=base,
                event_window_days=row.event_window_days if base else None,
            )
        )
    return targets


def build_preview_choices(
    *,
    ok_targets: list[GraphTarget],
    catalog_stations: list[tuple[str, str, str]],
    graph_key_to_display: dict[str, str],
    selected_station_pair: tuple[str, str] | None = None,
    selected_base_date: str | None = None,
) -> PreviewChoices:
    """OK対象からプレビュー用の候補一覧を生成する。"""

    station_name_by_pair = {
        (source, station_key): station_name
        for source, station_key, station_name in catalog_stations
    }
    station_display_to_pair: dict[str, tuple[str, str]] = {}
    for target in ok_targets:
        pair = (target.source, target.station_key)
        station_name = station_name_by_pair.get(pair, "")
        display = (
            f"{station_name} ({target.source}:{target.station_key})"
            if station_name
            else f"{target.source}:{target.station_key}"
        )
        station_display_to_pair[display] = pair

    station_values = sorted(station_display_to_pair.keys())
    date_values = sorted({t.base_date.isoformat() for t in ok_targets if t.base_date is not None})
    graph_targets = ok_targets
    if selected_station_pair is not None:
        graph_targets = [t for t in graph_targets if (t.source, t.station_key) == selected_station_pair]
    if selected_base_date:
        graph_targets = [t for t in graph_targets if t.base_date is not None and t.base_date.isoformat() == selected_base_date]
    graph_keys = sorted(
        {
            t.graph_type if t.event_window_days is None else f"{t.graph_type}:{t.event_window_days}day"
            for t in graph_targets
        }
    )
    graph_values = [graph_key_to_display.get(key, key) for key in graph_keys]
    graph_display_to_key = {
        graph_key_to_display.get(key, key): key
        for key in graph_keys
    }
    return PreviewChoices(
        station_display_to_pair=station_display_to_pair,
        graph_display_to_key=graph_display_to_key,
        station_values=station_values,
        date_values=date_values,
        graph_values=graph_values,
    )


def build_batch_targets(ok_targets: list[GraphTarget]) -> list[BatchTarget]:
    """GraphTarget 一覧を BatchTarget 一覧へ変換する。"""

    return [
        BatchTarget(
            source=t.source,
            station_key=t.station_key,
            graph_type=t.graph_type,
            base_datetime=t.base_date.isoformat() if t.base_date else None,
            event_window_days=t.event_window_days,
        )
        for t in ok_targets
    ]


def format_result_target_display(
    *,
    source: str,
    station_key: str,
    graph_type: str,
    base_datetime: str | None,
    event_window_days: int | None,
    catalog_stations: list[tuple[str, str, str]],
    source_label_map: dict[str, str],
    graph_label_map: dict[str, str],
) -> str:
    """結果一覧用の日本語ラベルを作る。"""

    station_name = _station_name_for_pair(catalog_stations, source, station_key)
    source_label = source_label_map.get(source, source)
    station_label = (
        f"{station_name}（{source_label}:{station_key}）"
        if station_name
        else f"{source_label}:{station_key}"
    )
    graph_label = graph_label_map.get(graph_type, graph_type)
    if event_window_days in (3, 5):
        base_label = _base_datetime_label(base_datetime)
        return f"{station_label} / {graph_label} / {base_label} / {event_window_days}日窓"
    if base_datetime == "annual" or event_window_days is None:
        return f"{station_label} / {graph_label} / 年最大"
    return f"{station_label} / {graph_label} / {_base_datetime_label(base_datetime)}"


def format_result_target_display_from_target_id(
    target_id: str,
    *,
    catalog_stations: list[tuple[str, str, str]],
    source_label_map: dict[str, str],
    graph_label_map: dict[str, str],
) -> str:
    """target_id から結果一覧用の日本語ラベルを作る。"""

    source, station_key, graph_type, base_datetime, event_window_days = _split_result_target_id(target_id)
    return format_result_target_display(
        source=source,
        station_key=station_key,
        graph_type=graph_type,
        base_datetime=base_datetime,
        event_window_days=event_window_days,
        catalog_stations=catalog_stations,
        source_label_map=source_label_map,
        graph_label_map=graph_label_map,
    )


def format_result_status_display(status: str) -> str:
    """結果一覧用の状態ラベルを日本語化する。"""

    text = str(status or "").strip()
    mapping = {
        "ok": "準備完了",
        "ng": "要確認",
        "ready": "準備完了",
        "precheck_ng": "要確認",
        "running": "実行中",
        "success": "完了",
        "failed": "失敗",
        "skipped": "スキップ",
    }
    return mapping.get(text, text)


def _station_name_for_pair(
    catalog_stations: list[tuple[str, str, str]],
    source: str,
    station_key: str,
) -> str:
    for catalog_source, catalog_station_key, station_name in catalog_stations:
        if catalog_source == source and catalog_station_key == station_key:
            return station_name
    return ""


def _base_datetime_label(base_datetime: str | None) -> str:
    if base_datetime in (None, "", "none"):
        return "基準日未指定"
    return base_datetime


def _split_result_target_id(target_id: str) -> tuple[str, str, str, str | None, int | None]:
    parts = target_id.split(":")
    if len(parts) == 4:
        source, station_key, graph_type, base_datetime = parts
        return source, station_key, graph_type, base_datetime, None
    if len(parts) == 5:
        source, station_key, graph_type, base_datetime, window = parts
        if window.endswith("day"):
            try:
                return source, station_key, graph_type, base_datetime, int(window[:-3])
            except ValueError:
                return source, station_key, graph_type, base_datetime, None
        return source, station_key, graph_type, base_datetime, None
    if len(parts) > 5:
        source, station_key, graph_type = parts[:3]
        base_datetime = parts[3]
        window = parts[4]
        if window.endswith("day"):
            try:
                return source, station_key, graph_type, base_datetime, int(window[:-3])
            except ValueError:
                return source, station_key, graph_type, base_datetime, None
        return source, station_key, graph_type, base_datetime, None
    if len(parts) == 3:
        source, station_key, graph_type = parts
        return source, station_key, graph_type, None, None
    return target_id, "", "", None, None
