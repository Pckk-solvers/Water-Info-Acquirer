import time
import tkinter as tk

import pandas as pd
import pytest

from src.water_info.ui.app import WWRApp
from src.water_info.ui import dam_panel
from src.water_info.service.dam import fetch_dam_data
from test_dam import CODE, getter, payload


def test_dam_on_existing_gui_target_validation_and_output(monkeypatch, tmp_path):
    try:
        root = tk.Tk()
    except tk.TclError:
        pytest.skip("Tk画面を作成できない環境")
    root.withdraw()
    app = WWRApp(root, lambda *a, **k: None, lambda *a, **k: None, ValueError, initial_codes=[CODE])
    app.root.withdraw()
    try:
        app.target.set("ダム諸量")
        app._select_target()
        assert app.regular_controls.winfo_manager() == ""
        assert app.dam_panel.winfo_manager() == "pack"
        panel = app.dam_panel
        panel.start_date.set("invalid")
        panel.start()
        assert "入力エラー" in panel.status.get() and not panel._running
        panel.start_date.set("2024-12-31")
        panel.end_date.set("2024-12-31")
        panel.output_dir.set(str(tmp_path))

        def local_fetch(code, request, **kwargs):
            return fetch_dam_data(code, request, get=getter(payload()), **kwargs)

        monkeypatch.setattr(dam_panel, "fetch_dam_data", local_fetch)
        panel.start()
        deadline = time.monotonic() + 20
        while panel._running and time.monotonic() < deadline:
            root.update()
            time.sleep(0.02)
        assert not panel._running
        assert "全区間取得完了" in panel.result_text.get("1.0", "end")
        books = list(tmp_path.rglob("*.xlsx"))
        assert len(books) == 1
        assert "2024" in pd.ExcelFile(books[0]).sheet_names
        assert list(tmp_path.rglob("*.parquet"))
        app.target.set("水位・流量・雨量")
        app._select_target()
        assert app.dam_panel.winfo_manager() == ""
        assert app.regular_controls.winfo_manager() == "pack"
        assert app._build_request().mode_type == "S"
    finally:
        app.root.destroy()
        root.destroy()
