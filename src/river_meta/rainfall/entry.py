from __future__ import annotations

import os
from collections.abc import Sequence


def _configure_runtime() -> None:
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_CACHE", "1")
    os.environ.setdefault("RIVER_RAINFALL_DISABLE_JMA_LOG_OUTPUT", "1")


def main(argv: Sequence[str] | None = None) -> int:
    _configure_runtime()
    args = list(argv) if argv is not None else []
    if not args or args == ["--gui"]:
        from .gui import main as gui_main

        return gui_main()

    cli_argv = [arg for arg in args if arg != "--gui"]
    from .cli import main as cli_main

    return cli_main(cli_argv)
