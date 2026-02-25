"""river-rainfall ランチャー。

使い方:
    uv run python river_rainfall.py --gui          # GUI モード
    uv run python river_rainfall.py [CLI引数...]    # CLI モード

引数なし、または --gui を指定するとGUIが起動します。
それ以外の引数が渡された場合はCLIモードで動作します。
"""

import sys


def main() -> int:
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
