"""river-rainfall ランチャー。

使い方:
    uv run python river_rainfall.py --gui          # GUI モード
    uv run python river_rainfall.py [CLI引数...]    # CLI モード

引数なし、または --gui を指定するとGUIが起動します。
それ以外の引数が渡された場合はCLIモードで動作します。
"""

import os
import sys
from pathlib import Path


def main() -> int:
    # 独自のsrcディレクトリをPYTHONPATH的に追加
    src_dir = Path(__file__).parent / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    # river_rainfall 実行時はJMA観測所キャッシュを作成しない
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_CACHE", "1")
    # river_rainfall 実行時はJMAログファイルを自動生成しない
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_LOG_OUTPUT", "1")
        
    # --gui フラグがあるか、引数がなければGUIモード
    args = sys.argv[1:]
    if not args or args == ["--gui"]:
        from river_meta.rainfall.gui import main as gui_main
        return gui_main()
    else:
        # --gui 以外の引数はそのままCLIに渡す
        argv = [a for a in args if a != "--gui"]
        from river_meta.rainfall.cli import main as cli_main
        return cli_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
