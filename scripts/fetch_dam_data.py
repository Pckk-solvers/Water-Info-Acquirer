"""GUIと同じ取得処理を使う、長期間取得・再開用の実行スクリプト。"""

import argparse
from datetime import date
import json
from pathlib import Path

from water_info.domain.dam import DamRequest
from water_info.infra.dam_writer import write_dam_outputs
from water_info.service.dam import fetch_dam_data


def main():
    parser = argparse.ArgumentParser(description="ダム諸量の時間値を取得・整理する")
    parser.add_argument("--code", required=True)
    parser.add_argument("--start", required=True, type=date.fromisoformat)
    parser.add_argument("--end", required=True, type=date.fromisoformat)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--parquet", action="store_true")
    args = parser.parse_args()
    request = DamRequest(args.start, args.end, args.output_dir.resolve(), args.parquet)

    def progress(n, total, report):
        print(json.dumps({"progress": n, "total": total, **report}, ensure_ascii=False), flush=True)

    data = fetch_dam_data(args.code, request, progress=progress)
    outputs = write_dam_outputs(data, progress=lambda s: print(s, flush=True))
    print(json.dumps({"status": data.status, "outputs": outputs}, ensure_ascii=False), flush=True)
    return 0 if data.status == "complete" else 2


if __name__ == "__main__":
    raise SystemExit(main())
