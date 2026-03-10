"""river-rainfall ランチャー。

使い方:
    uv run python river_rainfall.py --gui          # GUI モード
    uv run python river_rainfall.py [CLI引数...]    # CLI モード

引数なし、または --gui を指定するとGUIが起動します。
それ以外の引数が渡された場合はCLIモードで動作します。
"""

import sys
from pathlib import Path

src_dir = Path(__file__).resolve().parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from river_meta.rainfall.entry import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
