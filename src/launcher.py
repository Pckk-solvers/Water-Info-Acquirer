"""どのアプリを起動するか選ぶためのランチャーGUI。

PyInstaller 配布でも動くよう、サブプロセスではなく同一プロセス内で
対象モジュールを import して main() を呼び出す形にしている。
"""

import sys
import os
import tkinter as tk
import threading
import importlib
from pathlib import Path
from tkinter import messagebox

# 実行パスを EXE/スクリプトの親に合わせるためのルート決定
IS_FROZEN = getattr(sys, "frozen", False)
if IS_FROZEN:
    PROJECT_ROOT = Path(sys.executable).resolve().parent
else:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_DIR = PROJECT_ROOT / "src"


def _ensure_src_on_path() -> None:
    """凍結/非凍結どちらでも water_info / jma_rainfall_pipeline を見つけられるようにする。"""
    # 実行中のカレントディレクトリをプロジェクトルートに合わせておく
    try:
        os.chdir(PROJECT_ROOT)
    except OSError:
        pass  # 失敗しても致命的ではない

    src_path = str(SRC_DIR)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def _run_water(root: tk.Tk) -> None:
    """水文情報アプリを同一プロセスで起動する。"""
    try:
        root.destroy()
        _ensure_src_on_path()
        from water_info.__main__ import main as water_main

        water_main([])
    except Exception as exc:  # pragma: no cover - GUI only
        messagebox.showerror("起動エラー", f"water_info の起動に失敗しました:\n{exc}")


def _run_jma(root: tk.Tk) -> None:
    """JMA雨量パイプラインを同一プロセスで起動する。"""
    try:
        root.destroy()
        _ensure_src_on_path()
        from jma_rainfall_pipeline.main import main as jma_main

        jma_main()
    except Exception as exc:  # pragma: no cover - GUI only
        messagebox.showerror("起動エラー", f"jma_rainfall_pipeline の起動に失敗しました:\n{exc}")


def _preload_dependencies(status_label: tk.Label, buttons: list[tk.Button]) -> None:
    """裏で重い依存を読み込んでおき、子ウィンドウ起動を軽くする。"""
    _ensure_src_on_path()
    errors: list[str] = []
    try:
        importlib.import_module("water_info.__main__")
    except Exception as exc:
        errors.append(f"water_info のロードに失敗: {exc}")
    try:
        importlib.import_module("jma_rainfall_pipeline.main")
        importlib.import_module("jma_rainfall_pipeline.gui.app")
    except Exception as exc:
        errors.append(f"jma_rainfall_pipeline のロードに失敗: {exc}")

    def _finish() -> None:
        if errors:
            status_label.config(text="依存読み込みに失敗しました")
            messagebox.showerror("依存チェックエラー", "\n".join(errors))
        else:
            status_label.config(text="準備完了")
            for btn in buttons:
                btn.config(state=tk.NORMAL)

    status_label.after(0, _finish)


def main() -> None:
    root = tk.Tk()
    root.title("アプリ選択")
    root.geometry("320x180")

    tk.Label(root, text="起動するアプリを選んでください").pack(pady=8)
    status_label = tk.Label(root, text="依存を読み込み中です…")
    status_label.pack(pady=4)

    buttons: list[tk.Button] = []
    tk.Button(
        root,
        text="水文情報 (water_info)",
        state=tk.DISABLED,
        command=lambda: _run_water(root),
    ).pack(pady=6)

    tk.Button(
        root,
        text="JMA 雨量パイプライン",
        state=tk.DISABLED,
        command=lambda: _run_jma(root),
    ).pack(pady=6)

    # pack だけだと参照を残せないので直後に取り直す
    for child in root.winfo_children():
        if isinstance(child, tk.Button):
            buttons.append(child)

    threading.Thread(
        target=_preload_dependencies,
        args=(status_label, buttons),
        daemon=True,
    ).start()

    root.mainloop()


if __name__ == "__main__":
    main()
