from __future__ import annotations

from tkinter import messagebox

from hydrology_graphs.ui.preview_canvas import display_preview_image, show_preview_placeholder


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
        app.batch_tree.insert(
            "",
            "end",
            values=(row.target_id, row.status, row.reason_message or "", row.output_path or ""),
        )
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
