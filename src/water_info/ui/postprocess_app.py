"""位況・流況後処理GUI。"""

from __future__ import annotations

import queue
from pathlib import Path
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu

from ..postprocess_labels import (
    detect_metric_from_path,
    metric_choices,
    metric_definition,
    normalize_metric,
)
from ..postprocess_service import PostprocessRequest, PostprocessResult, run_postprocess


_EXCEL_FILETYPES = [
    ("Excelファイル", "*.xlsx"),
    ("すべてのファイル", "*.*"),
]


class PostprocessApp:
    """後処理の入力・実行・結果表示を担当するTkinter画面。"""

    def __init__(
        self,
        *,
        parent: tk.Misc,
        on_open_other=None,
        on_close=None,
        on_return_home=None,
    ) -> None:
        self.parent = parent
        self.on_open_other = on_open_other
        self.on_close = on_close
        self.on_return_home = on_return_home
        self._closed = False
        self._worker_thread: threading.Thread | None = None
        self._result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._metric_manually_selected = False

        self.root = tk.Toplevel(parent)
        self.root.title(get_module_title("postprocess", lang="jp"))
        self.root.geometry("900x650")
        self.root.minsize(760, 540)
        self.root.configure(bg="#eef2f7")
        self.root.protocol("WM_DELETE_WINDOW", self._handle_close)
        self.root.config(
            menu=build_navigation_menu(
                self.root,
                current_app_key="postprocess",
                on_open_other=self._open_other,
                on_return_home=self._return_home,
            )
        )

        self.metric_key = tk.StringVar(value="water_level")
        self.metric_display = tk.StringVar()
        self.metric_summary = tk.StringVar()
        self.label_preview = tk.StringVar()
        self.hour_label = tk.StringVar()
        self.daily_label = tk.StringVar()
        self.status = tk.StringVar(value="未実行")
        self.hour_file = tk.StringVar()
        self.daily_file = tk.StringVar()
        self.out_excel = tk.StringVar()
        self.out_parquet = tk.StringVar()
        self.parquet_enabled = tk.BooleanVar(value=False)

        self._build_ui()
        self._set_metric("water_level")
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

    def _open_other(self, app_key: str) -> None:
        if self.on_open_other is not None:
            self.root.destroy()
            self.on_open_other(app_key)

    def _return_home(self) -> None:
        self._closed = True
        try:
            self.root.destroy()
        finally:
            if self.on_return_home is not None:
                self.on_return_home()

    def _handle_close(self) -> None:
        self._closed = True
        try:
            self.root.destroy()
        finally:
            if self.on_close is not None:
                self.on_close()

    def _build_ui(self) -> None:
        outer = tk.Frame(self.root, bg="#eef2f7", padx=20, pady=16)
        outer.pack(fill="both", expand=True)

        tk.Label(
            outer,
            text=get_module_title("postprocess", lang="jp"),
            bg="#eef2f7",
            fg="#111827",
            font=("Yu Gothic UI", 20, "bold"),
        ).pack(anchor="w")
        tk.Label(
            outer,
            text="取得済みの時間・日データから位況または流況を算出します。",
            bg="#eef2f7",
            fg="#4b5563",
            font=("Yu Gothic UI", 10),
        ).pack(anchor="w", pady=(4, 14))

        content = ttk.Frame(outer)
        content.pack(fill="both", expand=True)

        input_frame = ttk.LabelFrame(content, text="対象種別と入出力", padding=12)
        input_frame.pack(fill="x")
        input_frame.columnconfigure(1, weight=1)

        ttk.Label(input_frame, text="対象種別").grid(row=0, column=0, sticky="w", padx=(0, 10), pady=5)
        self.metric_combo = ttk.Combobox(
            input_frame,
            textvariable=self.metric_display,
            values=[display for _key, display in metric_choices()],
            state="readonly",
            width=24,
        )
        self.metric_combo.grid(row=0, column=1, sticky="w", pady=5)
        self.metric_combo.bind("<<ComboboxSelected>>", self._on_metric_selected)
        ttk.Label(input_frame, textvariable=self.metric_summary).grid(
            row=0, column=2, sticky="w", padx=(12, 0), pady=5
        )

        ttk.Label(input_frame, textvariable=self.hour_label).grid(
            row=1, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self._add_path_row(
            input_frame,
            row=1,
            variable=self.hour_file,
            button_text="参照",
            command=self._select_hour_file,
        )

        ttk.Label(input_frame, textvariable=self.daily_label).grid(
            row=2, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self._add_path_row(
            input_frame,
            row=2,
            variable=self.daily_file,
            button_text="参照",
            command=self._select_daily_file,
        )

        ttk.Label(input_frame, text="Excel出力").grid(
            row=3, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self._add_path_row(
            input_frame,
            row=3,
            variable=self.out_excel,
            button_text="保存先",
            command=self._select_excel_output,
        )

        self.parquet_check = ttk.Checkbutton(
            input_frame,
            text="Parquet出力",
            variable=self.parquet_enabled,
            command=self._toggle_parquet,
        )
        self.parquet_check.grid(row=4, column=0, sticky="w", padx=(0, 10), pady=5)
        parquet_row = ttk.Frame(input_frame)
        parquet_row.grid(row=4, column=1, columnspan=2, sticky="ew", pady=5)
        parquet_row.columnconfigure(0, weight=1)
        self.parquet_entry = ttk.Entry(parquet_row, textvariable=self.out_parquet)
        self.parquet_entry.grid(row=0, column=0, sticky="ew")
        self.parquet_button = ttk.Button(parquet_row, text="参照", command=self._select_parquet_output)
        self.parquet_button.grid(row=0, column=1, padx=(6, 0))
        self._toggle_parquet()

        ttk.Label(
            content,
            textvariable=self.label_preview,
            foreground="#1d4ed8",
            wraplength=820,
        ).pack(anchor="w", pady=(10, 8))

        help_frame = ttk.LabelFrame(content, text="使い方", padding=10)
        help_frame.pack(fill="both", expand=True)
        help_text = (
            "1. 既存の水文データ取得GUIで作成した時間データ（_H）を選択します。\n"
            "2. 日データ（_D）がある場合は任意で選択します。\n"
            "3. 対象種別を確認し、Excel出力先を指定して実行します。\n"
            "ファイル名が _WH / _QH の場合は対象種別の初期値を補助的に設定します。"
        )
        ttk.Label(help_frame, text=help_text, justify="left", wraplength=820).pack(anchor="w", fill="x")

        action_frame = ttk.Frame(content)
        action_frame.pack(fill="x", pady=(12, 0))
        self.execute_button = ttk.Button(action_frame, text="実行", command=self._execute)
        self.execute_button.pack(side="left")
        ttk.Label(action_frame, textvariable=self.status).pack(side="left", padx=(12, 0))

    @staticmethod
    def _add_path_row(parent, *, row: int, variable: tk.StringVar, button_text: str, command) -> None:
        row_frame = ttk.Frame(parent)
        row_frame.grid(row=row, column=1, columnspan=2, sticky="ew", pady=5)
        row_frame.columnconfigure(0, weight=1)
        ttk.Entry(row_frame, textvariable=variable).grid(row=0, column=0, sticky="ew")
        ttk.Button(row_frame, text=button_text, command=command).grid(row=0, column=1, padx=(6, 0))

    def _on_metric_selected(self, _event=None) -> None:
        display = self.metric_display.get()
        for key, choice_display in metric_choices():
            if choice_display == display:
                self._metric_manually_selected = True
                self._set_metric(key)
                return

    def _set_metric(self, metric: str) -> None:
        key = normalize_metric(metric)
        definition = metric_definition(key)
        self.metric_key.set(key)
        self.metric_display.set(f"{definition.display_name}（{definition.statistic_name}）")
        self.metric_summary.set(f"単位: {definition.unit}")
        self.hour_label.set(f"{definition.display_name}時間データ (_H)")
        self.daily_label.set(f"{definition.display_name}日データ (_D, 任意)")
        self.label_preview.set(
            "出力ラベル例: "
            f"{definition.statistic_name}{definition.high_label} / "
            f"{definition.statistic_name}{definition.drought_label} / "
            f"{definition.max_value_label}"
        )

    def _select_hour_file(self) -> None:
        path = filedialog.askopenfilename(
            parent=self.root,
            title="時間データ（_H）を選択",
            filetypes=_EXCEL_FILETYPES,
        )
        if not path:
            return
        self.hour_file.set(path)
        if not self._metric_manually_selected:
            detected = detect_metric_from_path(path)
            if detected is not None:
                self._set_metric(detected)
        if not self.out_excel.get().strip():
            source = Path(path)
            self.out_excel.set(str(source.with_name(f"{source.stem}_postprocess.xlsx")))

    def _select_daily_file(self) -> None:
        path = filedialog.askopenfilename(
            parent=self.root,
            title="日データ（_D）を選択（任意）",
            filetypes=_EXCEL_FILETYPES,
        )
        if path:
            self.daily_file.set(path)

    def _select_excel_output(self) -> None:
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Excel出力先を選択",
            defaultextension=".xlsx",
            filetypes=_EXCEL_FILETYPES,
        )
        if path:
            self.out_excel.set(path)

    def _select_parquet_output(self) -> None:
        path = filedialog.askdirectory(parent=self.root, title="Parquet出力先を選択")
        if path:
            self.out_parquet.set(path)

    def _toggle_parquet(self) -> None:
        state = "normal" if self.parquet_enabled.get() else "disabled"
        self.parquet_entry.configure(state=state)
        self.parquet_button.configure(state=state)

    def _validate(self) -> PostprocessRequest | None:
        hour_file = Path(self.hour_file.get().strip())
        daily_raw = self.daily_file.get().strip()
        daily_file = Path(daily_raw) if daily_raw else None
        out_excel = Path(self.out_excel.get().strip())
        out_parquet_raw = self.out_parquet.get().strip() if self.parquet_enabled.get() else ""
        out_parquet = Path(out_parquet_raw) if out_parquet_raw else None

        errors: list[str] = []
        if not self.hour_file.get().strip():
            errors.append("時間データ（_H）を指定してください。")
        elif not hour_file.is_file():
            errors.append(f"時間データが見つかりません: {hour_file}")
        if daily_file is not None and not daily_file.is_file():
            errors.append(f"日データが見つかりません: {daily_file}")
        if not self.out_excel.get().strip():
            errors.append("Excel出力先を指定してください。")
        if self.parquet_enabled.get() and out_parquet is None:
            errors.append("Parquet出力先を指定してください。")
        if errors:
            messagebox.showerror("入力エラー", "\n".join(errors), parent=self.root)
            return None

        return PostprocessRequest(
            hour_file=hour_file,
            daily_file=daily_file,
            out_excel=out_excel,
            out_parquet=out_parquet,
            metric=normalize_metric(self.metric_key.get()),
        )

    def _execute(self) -> None:
        request = self._validate()
        if request is None or (self._worker_thread is not None and self._worker_thread.is_alive()):
            return
        definition = metric_definition(request.metric)
        self.execute_button.configure(state="disabled")
        self.status.set(f"{definition.statistic_name}を算出しています...")
        self._worker_thread = threading.Thread(target=self._worker, args=(request,), daemon=True)
        self._worker_thread.start()
        self.root.after(100, self._poll_result)

    def _worker(self, request: PostprocessRequest) -> None:
        try:
            result = run_postprocess(request)
        except Exception as exc:  # noqa: BLE001
            self._result_queue.put(("error", exc))
        else:
            self._result_queue.put(("done", result))

    def _poll_result(self) -> None:
        if self._closed:
            return
        try:
            kind, payload = self._result_queue.get_nowait()
        except queue.Empty:
            self.root.after(100, self._poll_result)
            return

        self.execute_button.configure(state="normal")
        if kind == "error":
            self.status.set("エラー")
            messagebox.showerror(
                "後処理エラー",
                f"後処理に失敗しました。\n\n{type(payload).__name__}: {payload}",
                parent=self.root,
            )
            return

        result = payload
        if not isinstance(result, PostprocessResult):
            self.status.set("エラー")
            messagebox.showerror("後処理エラー", "結果の形式が不正です。", parent=self.root)
            return
        definition = metric_definition(result.metric)
        self.status.set(f"{definition.statistic_name}の算出が完了しました")
        outputs = [f"Excel: {result.out_excel}"]
        if result.out_parquet is not None:
            outputs.append(f"Parquet: {result.out_parquet}")
        messagebox.showinfo("完了", "\n".join(outputs), parent=self.root)


def open_postprocess_app(
    *,
    parent: tk.Misc,
    on_open_other=None,
    on_close=None,
    on_return_home=None,
):
    """トップレベルランチャーから後処理GUIを開く。"""
    return PostprocessApp(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
    ).root
