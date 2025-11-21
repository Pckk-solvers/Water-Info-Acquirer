#!/usr/bin/env python3
"""JMA降雨量パイプライン - メインエントリーポイント"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence


def _ensure_src_on_path() -> None:
    """srcディレクトリをPythonパスに追加する。"""

    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


def _build_parser() -> argparse.ArgumentParser:
    """コマンドライン引数パーサーを構築する。"""

    parser = argparse.ArgumentParser(
        prog="jma-rainfall-pipeline",
        description="JMA降雨量パイプラインアプリケーションの起動スクリプト",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="バージョン情報を表示して終了します。",
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    """メイン処理を実行する。"""

    _ensure_src_on_path()
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.version:
        from jma_rainfall_pipeline.version import get_version_string

        print(get_version_string())
        return 0

    from jma_rainfall_pipeline.gui.app import main as gui_main

    gui_main()
    return 0


def main() -> None:
    """スクリプトのエントリーポイント。"""

    sys.exit(run(sys.argv[1:]))


if __name__ == "__main__":
    main()
