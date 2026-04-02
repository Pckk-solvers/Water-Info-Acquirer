from __future__ import annotations

import sys
from collections.abc import Sequence


_CLI_COMMANDS = {"list-prefectures", "list-stations", "fetch"}


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] in _CLI_COMMANDS:
        from .cli import main as cli_main

        return cli_main(args)

    from .main import run as gui_run

    return gui_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
