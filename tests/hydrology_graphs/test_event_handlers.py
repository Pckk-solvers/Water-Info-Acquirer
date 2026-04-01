from __future__ import annotations

from types import SimpleNamespace

from hydrology_graphs.ui import event_handlers


class _Tree:
    def __init__(self) -> None:
        self.rows: list[tuple] = []

    def insert(self, *_args, values):
        self.rows.append(values)


def _dummy_app():
    app = SimpleNamespace()
    app.batch_tree = _Tree()
    app.batch_status = SimpleNamespace(value="")
    app.batch_status.set = lambda v: setattr(app.batch_status, "value", v)
    app.preview_message = SimpleNamespace(value="")
    app.preview_message.set = lambda v: setattr(app.preview_message, "value", v)
    app._preview_running = True
    app._preview_image_bytes = b"x"
    app._preview_last_fit_size = (1, 1)
    app._preview_last_image_hash = 1
    app._append_log = lambda _m: None
    app._set_running_state = lambda _v: None
    app._set_scan_state = lambda _v: None
    app._apply_scan_result = lambda _v: None
    app.precheck_summary = SimpleNamespace(value="")
    app.precheck_summary.set = lambda v: setattr(app.precheck_summary, "value", v)
    app._start_preview_worker_if_needed = lambda: None
    return app


def test_handle_run_done_updates_rows_and_status():
    app = _dummy_app()
    result = SimpleNamespace(
        items=[SimpleNamespace(target_id="t", status="success", reason_message="", output_path="a.png")],
        summary=SimpleNamespace(success=1, failed=0, skipped=0),
    )
    event_handlers.handle_event(app, "run_done", result)
    assert len(app.batch_tree.rows) == 1
    assert app.batch_status.value.startswith("完了:")


def test_handle_preview_done_error_sets_message(monkeypatch):
    app = _dummy_app()
    monkeypatch.setattr(event_handlers, "show_preview_placeholder", lambda *_args, **_kwargs: None)
    result = SimpleNamespace(status="error", image_bytes_png=None, reason_message="err")
    event_handlers.handle_event(app, "preview_done", result)
    assert app._preview_running is False
    assert app.preview_message.value == "err"
