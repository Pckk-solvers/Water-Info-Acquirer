from __future__ import annotations

import argparse

from src.water_info_acquirer.launcher import main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Water-Info-Acquirer launcher")
    parser.add_argument("--dev", action="store_true", help="開発者モードで起動する")
    args = parser.parse_args()
    main(developer_mode=bool(args.dev))
