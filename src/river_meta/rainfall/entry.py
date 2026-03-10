from __future__ import annotations

import os
from collections.abc import Sequence


DEFAULT_PARQUET_DIR_PRIMARY = (
    r"Z:\1175D109_大阪狭山市におけるため池を考慮した浸水シミュレーション構築に関する検討業務"
    r"\50_作業\01_ochiai_temp\06_1974年-2025年_取得整形結果\parquet"
)
DEFAULT_PARQUET_DIR_SECONDARY = (
    r"Z:\1175D109_大阪狭山市におけるため池を考慮した浸水シミュレーション構築に関する検討業務"
    r"\50_作業\01_ochiai_temp\07_1974-2025-mizmizDB\parquet"
)


def _configure_runtime() -> None:
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_CACHE", "1")
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_LOG_OUTPUT", "1")


def main(argv: Sequence[str] | None = None) -> int:
    _configure_runtime()
    args = list(argv) if argv is not None else []
    if not args or args == ["--gui"]:
        from .gui import main as gui_main

        return gui_main(
            default_parquet_dir_primary=DEFAULT_PARQUET_DIR_PRIMARY,
            default_parquet_dir_secondary=DEFAULT_PARQUET_DIR_SECONDARY,
        )

    cli_argv = [arg for arg in args if arg != "--gui"]
    from .cli import main as cli_main

    return cli_main(cli_argv)
