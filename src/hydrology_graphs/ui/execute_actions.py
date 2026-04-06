from __future__ import annotations

import csv
import threading
from pathlib import Path
from tkinter import filedialog, messagebox

from hydrology_graphs.io.threshold_store import load_thresholds_with_cache
from hydrology_graphs.services import BatchRunInput, PrecheckInput
from hydrology_graphs.ui.view_models import (
    build_batch_targets,
    build_preview_choices,
    format_result_target_display,
    graph_targets_from_precheck_items,
)

_EVENT_GRAPH_TYPES = {"hyetograph", "hydrograph_discharge", "hydrograph_water_level"}


def run_precheck(app) -> None:
    """選択条件に対して実行前検証を行う。"""

    if app._scan_running:
        messagebox.showwarning("スキャン中", "スキャン完了後に実行前検証を行ってください。")
        return
    if getattr(app, "_station_selection_dirty", False):
        messagebox.showwarning("未反映", "観測所チェックが未反映です。先に「チェック反映」を押してください。")
        return
    graph_types, event_windows_by_graph = _selected_graph_matrix(app)
    if not graph_types:
        messagebox.showerror("入力エラー", "グラフ種別を1つ以上選択してください。")
        return
    station_pairs = app._selected_station_pairs_in_order()
    if not station_pairs:
        messagebox.showerror("入力エラー", "観測所を1つ以上選択してください。")
        return

    event_windows = sorted({day for days in event_windows_by_graph.values() for day in days})
    event_graph_selected = bool(event_windows_by_graph)

    base_dates = app.selected_base_dates
    if event_graph_selected and not base_dates:
        messagebox.showerror("入力エラー", "イベント系グラフを選択した場合は基準日を1つ以上追加してください。")
        return

    precheck_input = PrecheckInput(
        parquet_dir=app.parquet_dir.get().strip(),
        threshold_file_path=app.threshold_path.get().strip() or None,
        graph_types=graph_types,
        station_pairs=station_pairs,
        base_dates=base_dates,
        event_window_days_list=event_windows,
        event_window_days_by_graph=event_windows_by_graph,
        event_window_terminal_padding=bool(app.event_window_terminal_padding.get()),
    )
    app._append_log(
        f"[PRECHECK] start stations={len(station_pairs)} graph_types={len(graph_types)} base_dates={len(base_dates)} windows={event_windows}"
    )
    if not app._ensure_full_catalog_loaded():
        return
    threshold_file = app.threshold_path.get().strip() or None
    threshold_result = load_thresholds_with_cache(threshold_file, cache=app._threshold_cache)
    result = app.service.precheck_with_catalog(
        catalog=app._catalog,
        data=precheck_input,
        threshold_result=threshold_result,
    )

    app._precheck_ok_targets = graph_targets_from_precheck_items(items=result.items)
    _clear_result_rows(app)
    for row in result.items:
        reason = row.reason_message or ""
        status = "ready" if row.status == "ok" else "precheck_ng"
        display_target = format_result_target_display(
            source=row.source,
            station_key=row.station_key,
            graph_type=row.graph_type,
            base_datetime=row.base_datetime,
            event_window_days=row.event_window_days,
            catalog_stations=app._catalog_stations,
            source_label_map=app.SOURCE_LABELS,
            graph_label_map=app.GRAPH_TYPE_LABELS,
        )
        _upsert_result_row(
            app,
            target_id=row.target_id,
            display_target=display_target,
            window_days=row.event_window_days,
            status=status,
            reason=reason,
            output_path="",
        )
    app.precheck_summary.set(
        f"対象数: {result.summary.total_targets} / READY: {result.summary.ok_targets} / NG: {result.summary.ng_targets}"
    )
    app._append_log(
        f"[PRECHECK] done total={result.summary.total_targets} ready={result.summary.ok_targets} ng={result.summary.ng_targets}"
    )
    refresh_preview_choices(app)


def _selected_graph_matrix(app) -> tuple[list[str], dict[str, list[int]]]:
    """4列表のチェック状態から graph_types と窓指定を組み立てる。"""

    graph_types: list[str] = []
    event_windows_by_graph: dict[str, list[int]] = {}

    matrix = getattr(app, "graph_cell_vars", {})
    if not isinstance(matrix, dict):
        return graph_types, event_windows_by_graph

    annual_types = ("annual_max_rainfall", "annual_max_discharge", "annual_max_water_level")
    for graph_type in annual_types:
        var = matrix.get(graph_type)
        if var is not None and bool(var.get()):
            graph_types.append(graph_type)

    for event_graph in sorted(_EVENT_GRAPH_TYPES):
        windows: list[int] = []
        var_3 = matrix.get(f"{event_graph}:3day")
        if var_3 is not None and bool(var_3.get()):
            windows.append(3)
        var_5 = matrix.get(f"{event_graph}:5day")
        if var_5 is not None and bool(var_5.get()):
            windows.append(5)
        if windows:
            graph_types.append(event_graph)
            event_windows_by_graph[event_graph] = windows

    return graph_types, event_windows_by_graph


def refresh_preview_choices(app) -> None:
    """プレビューで選べる対象候補を更新する。"""

    if not app._precheck_ok_targets:
        app._clear_preview_choices()
        return
    choices = build_preview_choices(
        ok_targets=app._precheck_ok_targets,
        catalog_stations=app._catalog_stations,
        graph_key_to_display=app._preview_graph_key_to_display,
    )
    app._preview_station_display_to_pair = choices.station_display_to_pair
    app._preview_graph_display_to_key = choices.graph_display_to_key
    app.preview_station_combo.configure(values=choices.station_values)
    app.preview_date_combo.configure(values=choices.date_values)
    if not app.preview_target_station.get() and choices.station_values:
        app.preview_target_station.set(choices.station_values[0])
    if not app.preview_target_date.get() and choices.date_values:
        app.preview_target_date.set(choices.date_values[0])
    preview_graph_combo = getattr(app, "preview_graph_combo", None)
    if preview_graph_combo is not None:
        preview_graph_combo.configure(values=choices.graph_values)
        if app.preview_target_graph.get() not in choices.graph_values and choices.graph_values:
            app.preview_target_graph.set(choices.graph_values[0])
    app._refresh_style_forms_from_payload()


def start_batch_run(app) -> None:
    """バッチ実行を開始する。"""

    if app._running:
        return
    if app._scan_running:
        messagebox.showwarning("スキャン中", "スキャン完了後にバッチ実行してください。")
        return
    if not app._precheck_ok_targets:
        messagebox.showwarning("未検証", "実行前検証でREADY対象を作成してください。")
        return
    payload = app._style_from_editor()
    if payload is None:
        return
    if not app._confirm_default_style_before_run(payload):
        return
    if not app._ensure_full_catalog_loaded():
        return
    out_dir = filedialog.askdirectory(title="出力先フォルダを選択")
    if not out_dir:
        return
    app._style_payload = payload
    batch_targets = build_batch_targets(app._precheck_ok_targets)
    run_input = BatchRunInput(
        parquet_dir=app.parquet_dir.get().strip(),
        output_dir=out_dir,
        threshold_file_path=app.threshold_path.get().strip() or None,
        style_json_path=app._style_json_path,
        style_payload=payload,
        targets=batch_targets,
        event_window_terminal_padding=bool(app.event_window_terminal_padding.get()),
        should_stop=app._stop_event.is_set if app._stop_event else None,
    )
    for target in batch_targets:
        row_id = _row_id_for_target_id(target.target_id)
        if row_id in app._result_row_ids:
            app.result_tree.set(app._result_row_ids[row_id], "status", "running")
            app.result_tree.set(app._result_row_ids[row_id], "reason", "")
            app.result_tree.set(app._result_row_ids[row_id], "path", "")
    app._stop_event = threading.Event()
    app._set_running_state(True)
    app.batch_status.set("実行中...")
    app._append_log(f"[RUN] start targets={len(batch_targets)} out={out_dir}")

    def worker() -> None:
        try:
            result = app.service.run_batch(
                run_input,
                stop_requested=app._stop_event.is_set if app._stop_event else None,
            )
            app._event_queue.put(("run_done", result))
        except Exception as exc:  # noqa: BLE001
            app._event_queue.put(("run_error", str(exc)))

    threading.Thread(target=worker, daemon=True).start()


def add_base_date_from_candidate(app) -> None:
    """候補コンボから基準日を追加する。"""

    iso_date = app._current_base_date_candidate_iso()
    if not iso_date:
        return
    if iso_date in app.selected_base_dates:
        app._append_log(f"[BASE_DATE] skip duplicate {iso_date}")
        return
    app.selected_base_dates.append(iso_date)
    app.selected_base_dates.sort()
    app._append_log(f"[BASE_DATE] added {iso_date}")
    _sync_base_date_listbox(app)


def remove_selected_base_dates(app) -> None:
    """選択中の基準日を削除する。"""

    selected = [int(idx) for idx in app.base_date_list.curselection()]
    if not selected:
        return
    keep: list[str] = []
    selected_set = set(selected)
    for idx, value in enumerate(app.selected_base_dates):
        if idx in selected_set:
            continue
        keep.append(value)
    app.selected_base_dates = keep
    _sync_base_date_listbox(app)


def clear_base_dates(app) -> None:
    """基準日を全削除する。"""

    app.selected_base_dates = []
    _sync_base_date_listbox(app)


def export_base_dates_csv(app) -> None:
    """基準日リストをCSVへ保存する。"""

    if not app.selected_base_dates:
        messagebox.showwarning("基準日CSV", "保存する基準日がありません。")
        return
    path = filedialog.asksaveasfilename(
        title="基準日CSVを保存",
        defaultextension=".csv",
        filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
    )
    if not path:
        return
    file_path = Path(path)
    with file_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["base_date"])
        for value in app.selected_base_dates:
            writer.writerow([value])
    app._append_log(f"[BASE_DATE] exported {file_path}")


def import_base_dates_csv(app) -> None:
    """CSVから基準日リストを読み込む。"""

    path = filedialog.askopenfilename(
        title="基準日CSVを読込",
        filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
    )
    if not path:
        return
    file_path = Path(path)
    loaded: list[str] = []
    invalid_count = 0
    duplicate_count = 0
    with file_path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        if reader.fieldnames is None or "base_date" not in reader.fieldnames:
            messagebox.showerror("基準日CSV", "CSVヘッダに base_date 列が必要です。")
            return
        for row in reader:
            value = str(row.get("base_date", "")).strip()
            if not value:
                continue
            try:
                # 形式チェックのみ実施。保存時はISO文字列で保持する。
                from datetime import date

                date.fromisoformat(value)
            except ValueError:
                invalid_count += 1
                continue
            loaded.append(value)
    merged = list(app.selected_base_dates)
    for value in loaded:
        if value in merged:
            duplicate_count += 1
            continue
        merged.append(value)
    merged.sort()
    app.selected_base_dates = merged
    _sync_base_date_listbox(app)
    if invalid_count or duplicate_count:
        messagebox.showwarning(
            "基準日CSV",
            f"読込完了（不正: {invalid_count}件 / 重複: {duplicate_count}件 を除外）",
        )
    app._append_log(f"[BASE_DATE] imported {file_path} added={len(loaded) - duplicate_count}")


def _sync_base_date_listbox(app) -> None:
    app.base_date_list.delete(0, "end")
    for value in app.selected_base_dates:
        app.base_date_list.insert("end", value)


def _row_id_for_target_id(target_id: str) -> str:
    return target_id


def _clear_result_rows(app) -> None:
    app._result_row_ids = {}
    app._result_output_paths = {}
    for item_id in app.result_tree.get_children():
        app.result_tree.delete(item_id)


def _window_text(window_days: int | None) -> str:
    if window_days in (3, 5):
        return f"{window_days}day"
    return ""


def _upsert_result_row(
    app,
    *,
    target_id: str,
    display_target: str,
    window_days: int | None,
    status: str,
    reason: str,
    output_path: str,
) -> None:
    row_key = _row_id_for_target_id(target_id)
    values = (display_target, _window_text(window_days), status, reason)
    row_id = app._result_row_ids.get(row_key)
    if row_id is None:
        row_id = app.result_tree.insert("", "end", iid=row_key, values=values)
        app._result_row_ids[row_key] = row_id
    else:
        app.result_tree.item(row_id, values=values)
    if output_path:
        app._result_output_paths[row_key] = output_path
    else:
        app._result_output_paths.pop(row_key, None)
