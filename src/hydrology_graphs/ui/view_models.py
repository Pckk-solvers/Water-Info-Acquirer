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


def graph_targets_from_precheck_items(
    *,
    items: list[PrecheckItem],
    event_window_days: int,
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
                event_window_days=event_window_days if base else None,
            )
        )
    return targets


def build_preview_choices(
    *,
    ok_targets: list[GraphTarget],
    catalog_stations: list[tuple[str, str, str]],
    graph_key_to_display: dict[str, str],
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
    graph_keys = sorted({t.graph_type for t in ok_targets})
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
