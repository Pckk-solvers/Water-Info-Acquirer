from __future__ import annotations

import argparse
import sys
from typing import Sequence

from river_meta.services.gpkg import GpkgRunInput, run_csv_to_gpkg


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="river-gpkg",
        description="Convert station CSV to GPKG (EPSG:4326).",
    )
    parser.add_argument("--in-csv", required=True, help="入力CSVパス")
    parser.add_argument("--out-gpkg", required=True, help="出力GPKGパス")
    parser.add_argument("--layer", default="stations", help="GPKGレイヤ名")
    parser.add_argument("--lat-col", default="latitude", help="緯度列名")
    parser.add_argument("--lon-col", default="longitude", help="経度列名")
    parser.add_argument("--encoding", default="utf-8-sig", help="CSV文字コード")
    parser.add_argument(
        "--out-epsg",
        type=int,
        default=4326,
        help="出力EPSG（4326, 6669-6688）",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        run_csv_to_gpkg(
            GpkgRunInput(
                in_csv=args.in_csv,
                out_gpkg=args.out_gpkg,
                layer=args.layer,
                lat_col=args.lat_col,
                lon_col=args.lon_col,
                encoding=args.encoding,
                out_epsg=args.out_epsg,
            ),
            log=lambda msg: print(msg, file=sys.stderr),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[river-gpkg] fatal: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
