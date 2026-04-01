from __future__ import annotations

import threading
from tkinter import filedialog, messagebox
from hydrology_graphs.io.threshold_store import load_thresholds_with_cache
from hydrology_graphs.services import BatchRunInput, PrecheckInput
from hydrology_graphs.ui.view_models import (
    build_batch_targets,
    build_preview_choices,
    graph_targets_from_precheck_items,
    parse_base_dates_text,
    selected_station_pairs,
)


def run_precheck(app) -> None:
    """選択条件に対して実行前検証を行う。"""

    if app._scan_running:
        messagebox.showwarning("スキャン中", "スキャン完了後に実行前検証を行ってください。")
        return
    graph_types = [g for g, var in app.graph_type_vars.items() if var.get()]
    if not graph_types:
        messagebox.showerror("入力エラー", "グラフ種別を1つ以上選択してください。")
        return
    selected_indices = [int(idx) for idx in app.station_list.curselection()]
    if not selected_indices:
        messagebox.showerror("入力エラー", "観測所を1つ以上選択してください。")
        return
    station_pairs = selected_station_pairs(app._catalog_stations, selected_indices)
    base_dates = parse_base_dates_text(app.base_dates_text.get("1.0", "end"))
    precheck_input = PrecheckInput(
        parquet_dir=app.parquet_dir.get().strip(),
        threshold_file_path=app.threshold_path.get().strip() or None,
        graph_types=graph_types,
        station_pairs=station_pairs,
        base_dates=base_dates,
        event_window_days=int(app.event_window_days.get()),
    )
    app._append_log(
        f"[PRECHECK] start stations={len(station_pairs)} graph_types={len(graph_types)} base_dates={len(base_dates)}"
    )
    threshold_file = app.threshold_path.get().strip() or None
    threshold_result = load_thresholds_with_cache(threshold_file, cache=app._threshold_cache)
    if app._catalog is not None:
        result = app.service.precheck_with_catalog(
            catalog=app._catalog,
            data=precheck_input,
            threshold_result=threshold_result,
        )
    else:
        result = app.service.precheck(precheck_input)

    app._precheck_ok_targets = graph_targets_from_precheck_items(
        items=result.items,
        event_window_days=int(app.event_window_days.get()),
    )
    for item_id in app.precheck_tree.get_children():
        app.precheck_tree.delete(item_id)
    for row in result.items:
        reason = row.reason_message or ""
        app.precheck_tree.insert("", "end", values=(row.target_id, row.status, reason))
    app.precheck_summary.set(
        f"対象数: {result.summary.total_targets} / OK: {result.summary.ok_targets} / NG: {result.summary.ng_targets}"
    )
    app._append_log(
        f"[PRECHECK] done total={result.summary.total_targets} ok={result.summary.ok_targets} ng={result.summary.ng_targets}"
    )
    refresh_preview_choices(app)


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
        messagebox.showwarning("未検証", "実行前検証でOK対象を作成してください。")
        return
    out_dir = filedialog.askdirectory(title="出力先フォルダを選択")
    if not out_dir:
        return
    payload = app._style_from_editor()
    if payload is None:
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
        should_stop=app._stop_event.is_set if app._stop_event else None,
    )
    for item_id in app.batch_tree.get_children():
        app.batch_tree.delete(item_id)
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
