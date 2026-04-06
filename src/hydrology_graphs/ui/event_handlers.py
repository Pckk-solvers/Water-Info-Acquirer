from __future__ import annotations

from tkinter import messagebox

from hydrology_graphs.ui.preview_canvas import display_preview_image, show_preview_placeholder
from hydrology_graphs.ui.view_models import format_result_status_display, format_result_target_display_from_target_id


def handle_event(app, event: str, payload: object) -> None:
    """バックグラウンドイベントを種別ごとに反映する。"""

    if event == "scan_done":
        app._set_scan_state(False)
        app._apply_scan_result(payload)
    elif event == "scan_error":
        app._set_scan_state(False)
        app.batch_status.set("待機中")
        app.precheck_summary.set("対象数: 0 / NG: 0")
        app._append_log(f"[SCAN] error {payload}")
        messagebox.showerror("読込エラー", str(payload))
    elif event == "run_done":
        _handle_run_done(app, payload)
    elif event == "run_error":
        app.batch_status.set("エラー")
        app._append_log(f"[RUN] error {payload}")
        messagebox.showerror("バッチ実行エラー", str(payload))
        app._set_running_state(False)
    elif event == "preview_done":
        _handle_preview_done(app, payload)
    elif event == "preview_error":
        app._preview_running = False
        app._preview_image_bytes = None
        app._preview_last_fit_size = None
        app._preview_last_image_hash = None
        show_preview_placeholder(app, "プレビュー未生成")
        app.preview_message.set(str(payload))
        app._start_preview_worker_if_needed()


def _handle_run_done(app, payload: object) -> None:
    """バッチ完了イベントを反映する。"""

    result = payload
    for row in result.items:
        row_id = app._result_row_ids.get(row.target_id)
        display_target = format_result_target_display_from_target_id(
            row.target_id,
            catalog_stations=app._catalog_stations,
            source_label_map=app.SOURCE_LABELS,
            graph_label_map=app.GRAPH_TYPE_LABELS,
        )
        values = (display_target, format_result_status_display(row.status), row.reason_message or "")
        if row_id is None:
            row_id = app.result_tree.insert("", "end", iid=row.target_id, values=values)
            app._result_row_ids[row.target_id] = row_id
        else:
            app.result_tree.item(row_id, values=values)
        if row.output_path:
            app._result_output_paths[row.target_id] = row.output_path
        else:
            app._result_output_paths.pop(row.target_id, None)
    app.batch_status.set(
        f"完了: success={result.summary.success}, failed={result.summary.failed}, skipped={result.summary.skipped}"
    )
    app._append_log(
        f"[RUN] done success={result.summary.success} failed={result.summary.failed} skipped={result.summary.skipped}"
    )
    app._set_running_state(False)


def _handle_preview_done(app, payload: object) -> None:
    """プレビュー完了イベントを反映する。"""

    result = payload
    app._preview_running = False
    if result.status != "success" or result.image_bytes_png is None:
        app._preview_image_bytes = None
        app._preview_last_fit_size = None
        app._preview_last_image_hash = None
        show_preview_placeholder(app, "プレビュー未生成")
        app.preview_message.set(result.reason_message or "プレビュー生成に失敗しました。")
    else:
        display_preview_image(app, result.image_bytes_png, force=True)
        app.preview_message.set("プレビュー更新完了")
    app._start_preview_worker_if_needed()
