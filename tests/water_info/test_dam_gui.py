"""実際のTkウィジェットとワーカーを通した取得画面の確認。"""

from pathlib import Path
import threading
import time
import tkinter as tk

import pandas as pd
import pytest

from src.water_info import entry
from src.water_info.ui.app import WWRApp
from src.water_info.service import acquisition
from src.water_info.service.dam import fetch_dam_data
from test_dam import CODE, getter, payload


@pytest.fixture(scope="module")
def tk_root():
    try:
        root = tk.Tk()
    except tk.TclError:
        pytest.skip("Tk画面を作成できない環境")
    root.withdraw()
    yield root
    root.destroy()


@pytest.fixture
def app(tk_root):
    view = WWRApp(tk_root, lambda *a, **k: None, lambda *a, **k: None, ValueError)
    view.root.withdraw()
    yield view
    view.root.destroy()


def wait_done(app):
    deadline = time.monotonic() + 20
    while app._running and time.monotonic() < deadline:
        app.root.update()
        time.sleep(0.02)
    assert not app._running


def test_gui_station_validation_target_forms_and_help(app, tmp_path):
    app._on_execute()
    assert "観測所" in app.feedback.get()
    app.code_input.set(CODE + ",123\n" + CODE)
    app._add_code()
    assert app.codes == [CODE, "123"]
    app.code_input.set("１２３")
    app._add_code()
    assert "半角" in app.feedback.get()
    assert app.codes == [CODE, "123"]
    app.station_tree.selection_set("123")
    app._remove_code()
    assert app.codes == [CODE]
    app.target.set("ダム諸量")
    app._select_target()
    assert app.regular_controls.winfo_manager() == ""
    assert app.dam_panel.winfo_manager() == "pack"
    app.output_dir.set(str(tmp_path))
    app.dam_panel.start_date.set("2024-02-30")
    app._on_execute()
    assert "日付" in app.feedback.get() and not app._running
    app.dam_panel.start_date.set("2024-12-31")
    app.dam_panel.end_date.set("2024-12-30")
    app._on_execute()
    assert "逆転" in app.feedback.get()
    app.target.set("水位・流量・雨量")
    app._select_target()
    assert app.regular_controls.winfo_manager() == "pack"
    assert app.dam_panel.winfo_manager() == ""
    app.regular_controls.year_start.set("invalid")
    app._on_execute()
    assert "開始年" in app.feedback.get()
    app._show_help()
    assert app._help_window.winfo_exists()
    assert not app._help_window.grab_current()


def test_gui_dam_shared_output_results_and_input_lock(app, monkeypatch, tmp_path):
    app._insert_codes(CODE)
    app.target.set("ダム諸量")
    app._select_target()
    app.dam_panel.start_date.set("2024-12-31")
    app.dam_panel.end_date.set("2024-12-31")
    app.output_dir.set(str(tmp_path))
    app.export_parquet.set(True)

    def local_fetch(code, request, **kwargs):
        return fetch_dam_data(code, request, get=getter(payload()), **kwargs)

    monkeypatch.setattr(acquisition, "fetch_dam_data", local_fetch)
    app._on_execute()
    assert app._running
    assert str(app.folder_entry.cget("state")) == "disabled"
    assert str(app.execute_button.cget("state")) == "disabled"
    app.code_input.set("999")
    app._add_code()
    assert app.codes == [CODE]
    wait_done(app)
    assert app.results[CODE].status == "warning"
    assert "閉局" in app.result_detail.get("1.0", "end")
    assert app.results[CODE].excel_path.is_file()
    assert app.results[CODE].excel_path.is_relative_to(tmp_path)
    assert str(app.folder_entry.cget("state")) == "normal"
    assert str(app.open_excel_button.cget("state")) == "normal"
    assert len(list(tmp_path.rglob("*.xlsx"))) == 1
    assert list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("partial", [False, True])
def test_dam_complete_and_partial_are_displayed_separately(app, monkeypatch, tmp_path, partial):
    app._insert_codes(CODE)
    app.target.set("ダム諸量")
    app._select_target()
    app.dam_panel.start_date.set("2024-12-31")
    app.dam_panel.end_date.set("2024-12-31")
    app.output_dir.set(str(tmp_path))
    header = payload().splitlines()[:-1]
    rows = [payload(hour=f"{hour:02d}:00", values=["1", ""] * 5).splitlines()[-1] for hour in range(1, 25)]

    def get(*args, **kwargs):
        raise RuntimeError("source failed")

    def local_fetch(code, request, **kwargs):
        return fetch_dam_data(code, request, get=get if partial else getter(b"\n".join(header + rows)), **kwargs)

    monkeypatch.setattr(acquisition, "fetch_dam_data", local_fetch)
    app._on_execute()
    wait_done(app)
    assert app.results[CODE].status == ("partial" if partial else "complete")
    if partial:
        assert "source failed" in app.results[CODE].detail
    assert app.results[CODE].excel_path.is_file()


def test_minimum_size_keeps_forms_whole_and_output_accessible(app):
    app.target.set("ダム諸量")
    app._select_target()
    app.root.geometry("960x720")
    app.root.deiconify()
    app.root.update()
    assert app.dam_panel.winfo_height() >= app.dam_panel.winfo_reqheight()
    app.settings_canvas.yview_moveto(1)
    app.root.update()
    assert app.folder_entry.winfo_rooty() >= app.settings_canvas.winfo_rooty()
    assert app.folder_entry.winfo_rooty() + app.folder_entry.winfo_height() <= app.settings_canvas.winfo_rooty() + app.settings_canvas.winfo_height()
    assert app.result_detail.winfo_height() >= 50
    assert app.execute_button.winfo_rooty() + app.execute_button.winfo_height() <= app.root.winfo_rooty() + app.root.winfo_height()


@pytest.mark.parametrize("daily", [False, True])
def test_gui_normal_writes_actual_excel_and_parquet_to_selected_folder(app, monkeypatch, tmp_path, daily):
    app._insert_codes("123")
    app.output_dir.set(str(tmp_path / "selected"))
    app.export_parquet.set(True)
    form = app.regular_controls
    form.year_start.set("2024")
    form.year_end.set("2024")
    form.month_end.set("1月")
    form.interval.set("日値" if daily else "時間値")
    observed = []

    def source(**kwargs):
        observed.append(kwargs["should_stop"])
        kwargs["progress_callback"](increment=False, station_name="テスト観測所")
        kwargs["progress_callback"](increment=True)
        name = Path("123_テスト観測所_2024.xlsx")
        if daily:
            return pd.DataFrame({"水位": [1., 2.]}, index=pd.date_range("2024-01-01", periods=2)), name, "水位", "水位[m]"
        return pd.DataFrame({"datetime": pd.date_range("2024-01-01", periods=2, freq="h"),
                             "水位": [1., 2.], "sheet_year": [2024, 2024]}), name, "水位"

    monkeypatch.setattr(entry, "fetch_daily_dataframe_for_code" if daily else "fetch_hourly_dataframe_for_code", source)
    app.fetch_daily = entry.process_period_date_display_for_code
    app.fetch_hourly = entry.process_data_for_code
    app._on_execute()
    wait_done(app)
    result = app.results["123"]
    assert result.status == "complete", result.detail
    assert result.excel_path.parent == tmp_path / "selected"
    assert "2024年" in pd.ExcelFile(result.excel_path).sheet_names
    assert list((tmp_path / "selected" / "parquet").glob("*.parquet"))
    assert observed and callable(observed[0])


def test_gui_failure_can_retry_and_cancel_stops_remaining_stations(app, tmp_path):
    app._insert_codes("123 456")
    app.output_dir.set(str(tmp_path))

    def failure(*args, **kwargs):
        raise RuntimeError("test failure")

    app.fetch_hourly = failure
    app._on_execute()
    wait_done(app)
    assert {r.status for r in app.results.values()} == {"failed"}
    assert "test failure" in app.result_detail.get("1.0", "end")
    entered = threading.Event()

    def cancellable(*args, **kwargs):
        entered.set()
        deadline = time.monotonic() + 10
        while not kwargs["should_stop"]() and time.monotonic() < deadline:
            time.sleep(0.01)
        raise RuntimeError("cancelled")

    app.fetch_hourly = cancellable
    app._on_execute()
    assert entered.wait(timeout=2)
    app._cancel()
    wait_done(app)
    assert {r.status for r in app.results.values()} == {"cancelled"}
    assert app.results["456"].summary == "未実行"
    assert str(app.execute_button.cget("state")) == "normal"


def test_output_folder_validation_and_browse(app, monkeypatch, tmp_path):
    app._insert_codes(CODE)
    app.output_dir.set("")
    app._on_execute()
    assert "出力フォルダ" in app.feedback.get()
    file = tmp_path / "a.txt"
    file.write_text("test")
    app.output_dir.set(str(file))
    app._on_execute()
    assert "ファイルが指定" in app.feedback.get()
    monkeypatch.setattr("src.water_info.ui.app.filedialog.askdirectory", lambda **kw: str(tmp_path))
    app._browse_output()
    assert app.output_dir.get() == str(tmp_path)
