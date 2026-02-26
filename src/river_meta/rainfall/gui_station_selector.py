import json
import tkinter as tk
from pathlib import Path
from tkinter import ttk
from typing import Callable, Literal
from .tooltip import ToolTip


class StationSelector(ttk.Frame):
    """
    都道府県と観測所を選択する2ペイン構成のUI。
    取得元(JMA or water_info)によって表示する観測所データを切り替える。
    """

    def __init__(
        self,
        parent: tk.Widget,
        jma_json_path: str | Path,
        waterinfo_json_path: str | Path,
        on_selection_changed: Callable[[list[str]], None] | None = None,
    ) -> None:
        super().__init__(parent)
        # 都道府県は固定幅、候補/選択中を可変に配分
        self.columnconfigure(0, weight=0, minsize=180)
        self.columnconfigure(1, weight=2)
        self.columnconfigure(2, weight=2)
        self.rowconfigure(0, weight=1)
        self._enabled = True
        self._debounce_ms = 200
        self._pref_after_id: str | None = None
        self._station_after_id: str | None = None
        self._candidate_layout_after_id: str | None = None
        self._selected_layout_after_id: str | None = None

        self._jma_data: dict[str, list[dict]] = {}
        self._waterinfo_data: dict[str, dict] = {}
        self._current_source: Literal["jma", "water_info"] = "jma"

        self._stations_by_source_pref: dict[str, dict[str, list[dict[str, str]]]] = {
            "jma": {},
            "water_info": {},
        }
        self._station_meta_by_source: dict[str, dict[str, dict[str, str]]] = {
            "jma": {},
            "water_info": {},
        }
        self._selected_codes_by_source: dict[str, set[str]] = {
            "jma": set(),
            "water_info": set(),
        }
        self._tooltips: list[ToolTip] = []

        self.on_selection_changed = on_selection_changed

        self._load_data(jma_json_path, waterinfo_json_path)
        self._setup_button_color_style()
        self._build_ui()
        self._setup_tooltips()
        self._update_pref_list()
        self._update_selected_list()
        self.after_idle(self._apply_initial_tree_layout)

    def _setup_button_color_style(self) -> None:
        # サイズ・フォントは既定のままにし、色だけ変える
        self._button_style = "StationColor.TButton"
        style = ttk.Style(self)
        style.configure(self._button_style, background="#DDE8FF", foreground="#1F2937")
        style.map(
            self._button_style,
            background=[
                ("active", "#C9DBFF"),
                ("pressed", "#B7CEFF"),
                ("disabled", "#F1F5F9"),
            ],
            foreground=[("disabled", "#9CA3AF")],
        )

    def _load_data(self, jma_path: str | Path, waterinfo_path: str | Path) -> None:
        jma_file = Path(jma_path)
        if jma_file.exists():
            with open(jma_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self._jma_data = data.get("by_block_no", {})

        water_file = Path(waterinfo_path)
        if water_file.exists():
            with open(water_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                self._waterinfo_data = data.get("by_station_id", {})
        self._build_station_index()

    def _build_station_index(self) -> None:
        self._stations_by_source_pref = {"jma": {}, "water_info": {}}
        self._station_meta_by_source = {"jma": {}, "water_info": {}}

        for block_no, block_list in self._jma_data.items():
            for st in block_list:
                pref = str(st.get("pref_name") or "不明")
                station_name = str(st.get("station_name") or "")
                sid = st.get("station_id")
                identifier = f"{block_no} ({sid})" if sid else str(block_no)
                entry = {
                    "id": str(block_no),
                    "name": station_name,
                    "code_display": identifier,
                    "info": "",
                }
                self._stations_by_source_pref["jma"].setdefault(pref, []).append(entry)
                self._station_meta_by_source["jma"].setdefault(str(block_no), entry)

        for sid, st in self._waterinfo_data.items():
            pref = str(st.get("pref_name") or "不明")
            station_name = str(st.get("station_name") or "")
            suikei = str(st.get("suikei_name") or "")
            kasen = str(st.get("kasen_name") or "")
            info = f"{suikei} / {kasen}" if suikei or kasen else ""
            entry = {
                "id": str(sid),
                "name": station_name,
                "code_display": str(sid),
                "info": info,
            }
            self._stations_by_source_pref["water_info"].setdefault(pref, []).append(entry)
            self._station_meta_by_source["water_info"].setdefault(str(sid), entry)

    def _build_ui(self) -> None:
        # 左ペイン: 都道府県
        pref_frame = ttk.LabelFrame(self, text="都道府県", padding=6)
        pref_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 4))
        pref_frame.rowconfigure(1, weight=1)
        pref_frame.columnconfigure(0, weight=1)
        pref_frame.columnconfigure(1, weight=0)

        pref_toolbar = ttk.Frame(pref_frame)
        # 検索行はリスト本体(列0)と同幅にする。スクロールバー列は含めない。
        pref_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        pref_toolbar.columnconfigure(1, weight=1)

        self._pref_search_var = tk.StringVar()
        self._pref_search_var.trace_add("write", lambda *_: self._schedule_pref_update())
        ttk.Label(pref_toolbar, text="検索", width=6, anchor="e").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self._pref_search_entry = ttk.Entry(pref_toolbar, textvariable=self._pref_search_var)
        self._pref_search_entry.grid(row=0, column=1, sticky="ew")

        pref_scroll = ttk.Scrollbar(pref_frame, orient="vertical")
        self._pref_listbox = tk.Listbox(pref_frame, yscrollcommand=pref_scroll.set, exportselection=False)
        pref_scroll.config(command=self._pref_listbox.yview)
        self._pref_listbox.grid(row=1, column=0, sticky="nsew")
        pref_scroll.grid(row=1, column=1, sticky="ns")

        self._pref_listbox.bind("<<ListboxSelect>>", self._on_pref_selected)

        # 中央ペイン: 候補リスト
        candidate_frame = ttk.LabelFrame(self, text="候補観測所", padding=6)
        candidate_frame.grid(row=0, column=1, sticky="nsew", padx=4)
        candidate_frame.rowconfigure(1, weight=1)
        candidate_frame.columnconfigure(0, weight=1)

        toolbar = ttk.Frame(candidate_frame)
        toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        toolbar.columnconfigure(1, weight=1)
        toolbar.columnconfigure(2, weight=0)

        ttk.Label(toolbar, text="検索", width=6, anchor="e").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self._station_search_var = tk.StringVar()
        self._station_search_var.trace_add("write", lambda *_: self._schedule_station_update())
        self._station_search_entry = ttk.Entry(toolbar, textvariable=self._station_search_var)
        self._station_search_entry.grid(row=0, column=1, sticky="ew")

        btn_frame = ttk.Frame(toolbar)
        btn_frame.grid(row=0, column=2, sticky="e", padx=(8, 0))
        self._select_all_btn = ttk.Button(
            btn_frame,
            text="すべて選択",
            command=self._select_all_stations,
            style=self._button_style,
        )
        self._select_all_btn.pack(side="left", padx=(0, 4))
        self._deselect_all_btn = ttk.Button(
            btn_frame,
            text="表示中を解除",
            command=self._deselect_all_stations,
            style=self._button_style,
        )
        self._deselect_all_btn.pack(side="left")

        # 候補Treeview (クリックで選択/解除)
        columns = ("name", "code", "info")
        self._tree = ttk.Treeview(candidate_frame, columns=columns, show="headings", selectmode="none")
        self._tree.heading("name", text="観測所")
        self._tree.heading("code", text="コード")
        self._tree.heading("info", text="水系/河川 等")

        self._tree.column("name", width=170, minwidth=110, anchor="w", stretch=False)
        self._tree.column("code", width=120, minwidth=90, anchor="w", stretch=False)
        self._tree.column("info", width=250, minwidth=150, anchor="w", stretch=True)
        
        tree_scroll = ttk.Scrollbar(candidate_frame, orient="vertical", command=self._tree.yview)
        self._tree.configure(yscrollcommand=tree_scroll.set)

        self._tree.grid(row=1, column=0, sticky="nsew")
        tree_scroll.grid(row=1, column=1, sticky="ns")

        # 左右スクロール(JMAの名前が長すぎる場合など用)
        tree_xscroll = ttk.Scrollbar(candidate_frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(xscrollcommand=tree_xscroll.set)
        tree_xscroll.grid(row=2, column=0, sticky="ew")

        # Treeviewのクリックイベント（選択切替）
        self._tree.bind("<ButtonRelease-1>", self._on_tree_click)
        self._tree.bind("<Configure>", self._on_candidate_tree_configure, add="+")

        # 右ペイン: 選択中
        selected_frame = ttk.LabelFrame(self, text="選択中", padding=6)
        selected_frame.grid(row=0, column=2, sticky="nsew", padx=(4, 0))
        selected_frame.rowconfigure(1, weight=1)
        selected_frame.columnconfigure(0, weight=1)
        selected_frame.columnconfigure(1, weight=0)

        selected_toolbar = ttk.Frame(selected_frame)
        # ヘッダー行は選択中リスト本体(列0)と同幅にする。スクロールバー列は含めない。
        selected_toolbar.grid(row=0, column=0, sticky="ew", pady=(0, 4))
        selected_toolbar.columnconfigure(2, weight=1)

        ttk.Label(selected_toolbar, text="件数", width=6, anchor="e").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self._selected_count_var = tk.StringVar(value="0 件")
        ttk.Label(selected_toolbar, textvariable=self._selected_count_var).grid(row=0, column=1, sticky="w")

        selected_btn_frame = ttk.Frame(selected_toolbar)
        selected_btn_frame.grid(row=0, column=3, sticky="e")
        self._remove_selected_btn = ttk.Button(
            selected_btn_frame,
            text="選択行を解除",
            command=self._remove_selected_items,
            style=self._button_style,
        )
        self._remove_selected_btn.pack(side="left", padx=(0, 4))
        self._clear_selected_btn = ttk.Button(
            selected_btn_frame,
            text="全解除",
            command=self._clear_all_selected,
            style=self._button_style,
        )
        self._clear_selected_btn.pack(side="left")

        selected_columns = ("name", "code", "info")
        self._selected_tree = ttk.Treeview(
            selected_frame, columns=selected_columns, show="headings", selectmode="extended",
        )
        self._selected_tree.heading("name", text="選択中観測所")
        self._selected_tree.heading("code", text="コード")
        self._selected_tree.heading("info", text="情報")
        self._selected_tree.column("name", width=170, minwidth=110, anchor="w", stretch=False)
        self._selected_tree.column("code", width=120, minwidth=90, anchor="w", stretch=False)
        self._selected_tree.column("info", width=220, minwidth=120, anchor="w", stretch=True)
        self._selected_tree.grid(row=1, column=0, sticky="nsew")
        selected_scroll = ttk.Scrollbar(selected_frame, orient="vertical", command=self._selected_tree.yview)
        selected_scroll.grid(row=1, column=1, sticky="ns")
        self._selected_tree.configure(yscrollcommand=selected_scroll.set)
        self._selected_tree.bind("<Configure>", self._on_selected_tree_configure, add="+")

        manual_frame = ttk.Frame(selected_frame)
        manual_frame.grid(row=2, column=0, sticky="ew", pady=(4, 0))
        manual_frame.columnconfigure(1, weight=1)
        self._manual_var = tk.StringVar()
        ttk.Label(manual_frame, text="観測所コード", anchor="e").grid(row=0, column=0, sticky="e", padx=(0, 6))
        self._manual_entry = ttk.Entry(manual_frame, textvariable=self._manual_var)
        self._manual_entry.grid(row=0, column=1, sticky="ew")
        self._manual_add_btn = ttk.Button(
            manual_frame,
            text="選択に追加",
            command=self._add_manual_codes,
            style=self._button_style,
        )
        self._manual_add_btn.grid(row=0, column=2, padx=(4, 0))
        self._manual_entry.bind("<Return>", self._on_manual_enter)

    def _setup_tooltips(self) -> None:
        self._tooltips.extend(
            [
                ToolTip(self._tree, "候補の行をクリックすると選択/解除を切り替えます。"),
                ToolTip(self._remove_selected_btn, "「選択中」一覧で選択した観測所だけを解除します（複数選択可）。"),
                ToolTip(self._manual_entry, "観測所コードをカンマ区切りで入力できます（例: 47401,62078）。"),
                ToolTip(self._manual_add_btn, "入力したコードを選択一覧へ追加します。"),
            ]
        )

    def set_source(self, source: Literal["jma", "water_info"]) -> None:
        """取得元が切り替わったときに呼ばれる"""
        if self._current_source != source:
            self._cancel_scheduled_updates()
            self._current_source = source
            self._pref_search_var.set("")
            self._station_search_var.set("")
            self._update_pref_list()
            self._update_station_list()
            self._update_selected_list()
            self._notify_change()
            self._cancel_scheduled_updates()

    def _cancel_scheduled_updates(self) -> None:
        if self._pref_after_id is not None:
            try:
                self.after_cancel(self._pref_after_id)
            except tk.TclError:
                pass
            self._pref_after_id = None
        if self._station_after_id is not None:
            try:
                self.after_cancel(self._station_after_id)
            except tk.TclError:
                pass
            self._station_after_id = None
        if self._candidate_layout_after_id is not None:
            try:
                self.after_cancel(self._candidate_layout_after_id)
            except tk.TclError:
                pass
            self._candidate_layout_after_id = None
        if self._selected_layout_after_id is not None:
            try:
                self.after_cancel(self._selected_layout_after_id)
            except tk.TclError:
                pass
            self._selected_layout_after_id = None

    def _schedule_pref_update(self) -> None:
        if self._pref_after_id is not None:
            try:
                self.after_cancel(self._pref_after_id)
            except tk.TclError:
                pass
        self._pref_after_id = self.after(self._debounce_ms, self._run_pref_update)

    def _run_pref_update(self) -> None:
        self._pref_after_id = None
        self._update_pref_list()

    def _schedule_station_update(self) -> None:
        if self._station_after_id is not None:
            try:
                self.after_cancel(self._station_after_id)
            except tk.TclError:
                pass
        self._station_after_id = self.after(self._debounce_ms, self._run_station_update)

    def _run_station_update(self) -> None:
        self._station_after_id = None
        self._update_station_list()

    def _get_current_prefs(self) -> list[str]:
        prefs = self._stations_by_source_pref.get(self._current_source, {}).keys()
        return sorted(str(p) for p in prefs)

    def _update_pref_list(self) -> None:
        old_selected = self._get_selected_pref()
        self._pref_listbox.delete(0, tk.END)
        prefs = self._get_current_prefs()
        query = self._pref_search_var.get().strip().lower()

        filtered: list[str] = []
        for p in prefs:
            if not query or query in p.lower():
                self._pref_listbox.insert(tk.END, p)
                filtered.append(p)

        if filtered:
            if old_selected in filtered:
                idx = filtered.index(old_selected)
            else:
                idx = 0
            self._pref_listbox.selection_clear(0, tk.END)
            self._pref_listbox.selection_set(idx)
            self._pref_listbox.activate(idx)

        self._update_station_list()

    def _get_selected_pref(self) -> str | None:
        selection = self._pref_listbox.curselection()
        if not selection:
            return None
        return self._pref_listbox.get(selection[0])

    def _on_pref_selected(self, event) -> None:
        if not self._enabled:
            return
        self._station_search_var.set("")
        if self._station_after_id is not None:
            try:
                self.after_cancel(self._station_after_id)
            except tk.TclError:
                pass
            self._station_after_id = None
        self._update_station_list()

    def _update_station_list(self) -> None:
        for item in self._tree.get_children():
            self._tree.delete(item)

        pref = self._get_selected_pref()
        if not pref:
            return

        query = self._station_search_var.get().strip().lower()
        stations = self._stations_by_source_pref.get(self._current_source, {}).get(pref, [])
        filtered: list[dict[str, str]] = []
        for s in stations:
            if (
                not query
                or query in s["name"].lower()
                or query in s["code_display"].lower()
                or query in s["info"].lower()
            ):
                filtered.append(s)

        filtered.sort(key=lambda x: x["name"])
        selected_set = self._selected_codes_by_source[self._current_source]

        for s in filtered:
            is_checked = s["id"] in selected_set
            check_mark = "☑" if is_checked else "☐"
            name_display = f"{check_mark} {s['name']}"

            self._tree.insert(
                "", tk.END,
                values=(name_display, s["code_display"], s["info"]),
                tags=(s["id"],)
            )
        self._schedule_candidate_layout()

    def _on_tree_click(self, event) -> None:
        if not self._enabled:
            return
        region = self._tree.identify_region(event.x, event.y)
        if region != "cell":
            return
            
        col = self._tree.identify_column(event.x)
        if col != "#1": # 名前カラム（チェックボックスがある列）以外は無視
            return
            
        item_id = self._tree.identify_row(event.y)
        if not item_id:
            return
            
        pref = self._get_selected_pref()
        if not pref:
            return

        tags = self._tree.item(item_id, "tags")
        if not tags:
            return

        station_id = str(tags[0])
        selected_set = self._selected_codes_by_source[self._current_source]
        if station_id in selected_set:
            selected_set.remove(station_id)
        else:
            selected_set.add(station_id)

        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _select_all_stations(self) -> None:
        if not self._enabled:
            return
        selected_set = self._selected_codes_by_source[self._current_source]
        changed = False
        for item in self._tree.get_children():
            tags = self._tree.item(item, "tags")
            if not tags:
                continue
            station_id = str(tags[0])
            if station_id not in selected_set:
                selected_set.add(station_id)
                changed = True
        if not changed:
            return
        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _deselect_all_stations(self) -> None:
        if not self._enabled:
            return
        selected_set = self._selected_codes_by_source[self._current_source]
        changed = False
        for item in self._tree.get_children():
            tags = self._tree.item(item, "tags")
            if not tags:
                continue
            station_id = str(tags[0])
            if station_id in selected_set:
                selected_set.remove(station_id)
                changed = True
        if not changed:
            return
        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _update_selected_list(self) -> None:
        for item in self._selected_tree.get_children():
            self._selected_tree.delete(item)
        selected_set = self._selected_codes_by_source[self._current_source]
        rows: list[tuple[str, str, dict[str, str]]] = []
        for code in selected_set:
            meta = self._get_station_meta(self._current_source, code)
            rows.append((meta["name"], code, meta))
        rows.sort(key=lambda x: (x[0], x[1]))
        for _, code, meta in rows:
            self._selected_tree.insert(
                "",
                tk.END,
                values=(meta["name"], meta["code_display"], meta["info"]),
                tags=(code,),
            )
        self._selected_count_var.set(f"{len(selected_set)} 件")
        self._schedule_selected_layout()

    def _get_station_meta(self, source: str, code: str) -> dict[str, str]:
        found = self._station_meta_by_source.get(source, {}).get(code)
        if found is not None:
            return found
        return {
            "id": code,
            "name": "(手入力)",
            "code_display": code,
            "info": "手入力",
        }

    def _remove_selected_items(self) -> None:
        if not self._enabled:
            return
        selected_rows = self._selected_tree.selection()
        if not selected_rows:
            return
        selected_set = self._selected_codes_by_source[self._current_source]
        changed = False
        for row in selected_rows:
            tags = self._selected_tree.item(row, "tags")
            if not tags:
                continue
            code = str(tags[0])
            if code in selected_set:
                selected_set.remove(code)
                changed = True
        if not changed:
            return
        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _clear_all_selected(self) -> None:
        if not self._enabled:
            return
        selected_set = self._selected_codes_by_source[self._current_source]
        if not selected_set:
            return
        selected_set.clear()
        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _on_manual_enter(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._add_manual_codes()

    def _add_manual_codes(self) -> None:
        if not self._enabled:
            return
        raw = self._manual_var.get().strip()
        if not raw:
            return
        selected_set = self._selected_codes_by_source[self._current_source]
        changed = False
        for token in raw.split(","):
            code = token.strip()
            if not code:
                continue
            if code not in selected_set:
                selected_set.add(code)
                changed = True
        self._manual_var.set("")
        if not changed:
            return
        self._update_station_list()
        self._update_selected_list()
        self._notify_change()

    def _notify_change(self) -> None:
        if self.on_selection_changed:
            self.on_selection_changed(self.get_selected_codes())

    def get_selected_codes(self) -> list[str]:
        return sorted(self._selected_codes_by_source[self._current_source])

    def set_manual_codes(self, raw_str: str) -> None:
        self._manual_var.set(raw_str)
        self._add_manual_codes()

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        state = "normal" if enabled else "disabled"
        self._pref_search_entry.configure(state=state)
        self._station_search_entry.configure(state=state)
        self._manual_entry.configure(state=state)
        self._manual_add_btn.configure(state=state)
        self._select_all_btn.configure(state=state)
        self._deselect_all_btn.configure(state=state)
        self._remove_selected_btn.configure(state=state)
        self._clear_selected_btn.configure(state=state)
        self._pref_listbox.configure(state=state)
        if enabled:
            self._tree.state(["!disabled"])
            self._selected_tree.state(["!disabled"])
        else:
            self._tree.state(["disabled"])
            self._selected_tree.state(["disabled"])

    def refresh_layout(self) -> None:
        self.update_idletasks()
        self._pref_listbox.update_idletasks()
        self._tree.update_idletasks()
        self._selected_tree.update_idletasks()
        self._apply_candidate_tree_layout()
        self._apply_selected_tree_layout()

    def _apply_initial_tree_layout(self) -> None:
        self.update_idletasks()
        self._apply_candidate_tree_layout()
        self._apply_selected_tree_layout()

    def _on_candidate_tree_configure(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._schedule_candidate_layout()

    def _on_selected_tree_configure(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._schedule_selected_layout()

    def _schedule_candidate_layout(self) -> None:
        if self._candidate_layout_after_id is not None:
            return
        self._candidate_layout_after_id = self.after_idle(self._apply_candidate_tree_layout)

    def _schedule_selected_layout(self) -> None:
        if self._selected_layout_after_id is not None:
            return
        self._selected_layout_after_id = self.after_idle(self._apply_selected_tree_layout)

    def _apply_candidate_tree_layout(self) -> None:
        self._candidate_layout_after_id = None
        self._fit_tree_columns(
            self._tree,
            min_name=170,
            min_code=120,
            min_info=190,
            name_extra_ratio=0.25,
        )

    def _apply_selected_tree_layout(self) -> None:
        self._selected_layout_after_id = None
        self._fit_tree_columns(
            self._selected_tree,
            min_name=180,
            min_code=120,
            min_info=170,
            name_extra_ratio=0.30,
        )

    def _fit_tree_columns(
        self,
        tree: ttk.Treeview,
        *,
        min_name: int,
        min_code: int,
        min_info: int,
        name_extra_ratio: float,
    ) -> None:
        available = max(tree.winfo_width() - 6, 0)
        if available <= 0:
            return

        min_total = min_name + min_code + min_info
        if available <= min_total:
            code_width = min(min_code, max(90, int(available * 0.26)))
            remaining = max(available - code_width, 170)
            name_width = max(100, int(remaining * 0.42))
            info_width = max(70, remaining - name_width)
        else:
            extra = available - min_total
            code_width = min_code
            name_width = min_name + int(extra * name_extra_ratio)
            info_width = max(min_info, available - code_width - name_width)

        tree.column("name", width=name_width, minwidth=100, anchor="w", stretch=False)
        tree.column("code", width=code_width, minwidth=90, anchor="w", stretch=False)
        tree.column("info", width=info_width, minwidth=70, anchor="w", stretch=True)
