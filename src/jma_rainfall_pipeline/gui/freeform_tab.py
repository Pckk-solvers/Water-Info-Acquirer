"""Freeform タブ: 手入力から観測地点を特定する GUI."""

from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Dict, List, Tuple

from jma_rainfall_pipeline.fetcher.freeform_parser import (
    FreeformParserError,
    parse_freeform,
)
from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_prefecture_codes
from jma_rainfall_pipeline.fetcher.selection_builder import build_station_list
from jma_rainfall_pipeline.utils.cache_manager import CACHE_MANAGER

from .error_dialog import show_error


class FreeformTab(ttk.Frame):
    """フリーフォーム入力タブ."""

    def __init__(self, parent: tk.Widget) -> None:
        super().__init__(parent)

        # 都道府県の一覧を初期ロード
        self._pref_pairs: List[Tuple[str, str]] = self._load_prefectures()
        self.code_to_pref: Dict[str, str] = {code: name for code, name in self._pref_pairs}
        self.prefecture_map: Dict[str, str] = self._build_prefecture_map(self._pref_pairs)

        # 観測所データのキャッシュ
        self.station_map: Dict[Tuple[str, str], str] = {}
        self.reverse_station_map: Dict[str, List[Tuple[str, str]]] = {}
        self.status_var = tk.StringVar(value="")
        self._progress: ttk.Progressbar | None = None

        self._build_ui()

    # ------------------------------------------------------------------
    # 初期データのロード
    # ------------------------------------------------------------------
    def _load_prefectures(self) -> List[Tuple[str, str]]:
        try:
            pairs = [(code.zfill(2), name) for code, name in fetch_prefecture_codes()]
            if not pairs:
                raise ValueError("都道府県一覧を取得できませんでした")
            return pairs
        except Exception as exc:
            show_error(
                self.master,
                "都道府県取得エラー",
                "都道府県一覧の取得に失敗しました。",
                exc,
            )
            return []

    def _build_prefecture_map(self, pairs: List[Tuple[str, str]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        for code, name in pairs:
            normalized = code.zfill(2)
            mapping[normalized] = normalized
            mapping[normalized.lstrip("0") or normalized] = normalized
            mapping[name] = normalized
        return mapping

    # ------------------------------------------------------------------
    # GUI レイアウト
    # ------------------------------------------------------------------
    def _build_ui(self) -> None:
        ttk.Label(self, text="都道府県 (例: 埼玉, 11, 東京):").pack(anchor=tk.W, pady=(10, 0), padx=10)
        self.pref_entry = ttk.Entry(self)
        self.pref_entry.pack(fill=tk.X, padx=10)

        ttk.Label(self, text="観測所 (例: 田無, 47401, つくば):").pack(anchor=tk.W, pady=(10, 0), padx=10)
        self.sta_entry = ttk.Entry(self)
        self.sta_entry.pack(fill=tk.X, padx=10)

        ttk.Button(self, text="解析して確認", command=self._on_confirm).pack(pady=5)

        self.freeform_output = tk.Text(self, height=10)
        self.freeform_output.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.status_label = ttk.Label(self, textvariable=self.status_var, foreground="gray")
        self.status_label.pack(anchor=tk.W, padx=10, pady=(0, 5))

        self._progress = ttk.Progressbar(self, mode="indeterminate")

    # ------------------------------------------------------------------
    # 観測所データの準備
    # ------------------------------------------------------------------
    def _ensure_station_maps(self) -> None:
        if self.station_map:
            return

        if not self.code_to_pref:
            raise RuntimeError("都道府県データを取得できませんでした")

        cache_hint = ""
        if CACHE_MANAGER.enabled and self.code_to_pref:
            sample_code = next(iter(self.code_to_pref.keys()))
            entry = CACHE_MANAGER.load_stations(sample_code)
            if entry and not entry.expired:
                cache_hint = " (キャッシュ利用)"

        self._start_progress(f"観測所一覧を取得しています...{cache_hint}")

        try:
            station_records = build_station_list(list(self.code_to_pref.keys()))
        except Exception as exc:
            self._stop_progress("観測所の取得に失敗しました")
            show_error(
                self.master,
                "観測所リスト取得エラー",
                "観測所リストの取得中にエラーが発生しました。",
                exc,
            )
            raise

        for rec in station_records:
            prec_no = str(rec.get("prec_no", "")).zfill(2)
            station_name = rec.get("station", "").strip()
            block_no = str(rec.get("block_no", "")).strip()
            if not prec_no or not station_name or not block_no:
                continue

            key = (prec_no, station_name)
            self.station_map[key] = block_no

            candidates = self.reverse_station_map.setdefault(station_name, [])
            candidate = (prec_no, block_no)
            if candidate not in candidates:
                candidates.append(candidate)

        total_items = len(self.station_map)
        status_message = f"観測所データを読み込みました ({total_items}件)"
        self._stop_progress(status_message)

    # ------------------------------------------------------------------
    # プログレス表示
    # ------------------------------------------------------------------
    def _start_progress(self, message: str) -> None:
        if self._progress is None:
            return
        self.status_var.set(message)
        if not self._progress.winfo_ismapped():
            self._progress.pack(fill=tk.X, padx=10, pady=(0, 5))
        self._progress.start(50)
        self.update_idletasks()

    def _stop_progress(self, message: str = "") -> None:
        if self._progress is None:
            return
        self._progress.stop()
        if self._progress.winfo_ismapped():
            self._progress.pack_forget()
        self.status_var.set(message)
        self.update_idletasks()

    # ------------------------------------------------------------------
    # イベントハンドラ
    # ------------------------------------------------------------------
    def _on_confirm(self) -> None:
        """フリーフォーム入力を解析する."""
        pref_txt = self.pref_entry.get().strip()
        sta_txt = self.sta_entry.get().strip()

        if not pref_txt or not sta_txt:
            show_error(
                self.master,
                "入力エラー",
                "都道府県と観測所の両方を入力してください。",
                None,
            )
            return

        try:
            self._ensure_station_maps()
        except Exception:
            return

        try:
            parsed = parse_freeform(
                pref_txt,
                sta_txt,
                self.prefecture_map,
                self.station_map,
                self.reverse_station_map,
            )

            self.freeform_output.delete("1.0", tk.END)
            if not parsed:
                self.status_var.set("入力内容に一致する観測所は見つかりませんでした")
                self.freeform_output.insert(tk.END, "条件に合致する観測所が見つかりませんでした。")
                return

            for rec in parsed:
                prec_no = str(rec.get("prec_no", "")).zfill(2)
                pref_name = self.code_to_pref.get(prec_no, prec_no)
                station_name = rec.get("station", "")
                block_no = rec.get("block_no", "")
                obs = rec.get("obs_method", "不明") or "不明"
                line = f"{pref_name} ({prec_no}) - {station_name} ({block_no}) - 観測種別: {obs}"
                self.freeform_output.insert(tk.END, f"{line}\n")

            self.status_var.set(f"入力内容から {len(parsed)} 件を特定しました")

        except FreeformParserError as exc:
            self.status_var.set("入力内容の解析に失敗しました")
            show_error(
                self.master,
                "フリーフォーム解析エラー",
                "入力テキストの解析中にエラーが発生しました。",
                exc,
            )
        except Exception as exc:
            self.status_var.set("予期しないエラーが発生しました")
            show_error(
                self.master,
                "予期しないエラー",
                "観測所情報の取得中にエラーが発生しました。",
                exc,
            )
