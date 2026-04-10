from __future__ import annotations

import argparse

from src.water_info_acquirer.app_registry import APP_DEFINITIONS
from src.water_info_acquirer.launcher import main


def _build_parser() -> argparse.ArgumentParser:
    app_keys = tuple(definition.key for definition in APP_DEFINITIONS if definition.enabled)
    parser = argparse.ArgumentParser(description="Water-Info-Acquirer launcher")
    parser.add_argument("--dev", action="store_true", help="開発者モードで起動する")
    parser.add_argument("--app", dest="app_opt", choices=app_keys, help="指定アプリをランチャー経由せず直接起動する")
    parser.add_argument("app_pos", nargs="?", choices=app_keys, help="指定アプリをランチャー経由せず直接起動する")
    return parser


def _resolve_launch_target(args: argparse.Namespace) -> str | None:
    return args.app_opt or args.app_pos


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    main(developer_mode=bool(args.dev), launch_target=_resolve_launch_target(args))
