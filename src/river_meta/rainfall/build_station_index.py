from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_prefecture_codes, fetch_station_codes

from .station_index import default_station_index_path


def _to_obs_type(obs_method: str) -> str:
    token = (obs_method or "").strip().lower()
    if token == "s":
        return "s1"
    if token == "a":
        return "a1"
    if token.endswith("1"):
        return token
    return "a1"


def build_jma_station_index(*, output_path: str | None = None) -> Path:
    out_path = Path(output_path) if output_path else default_station_index_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    prefectures = fetch_prefecture_codes()
    by_block_no: dict[str, list[dict[str, str]]] = {}
    station_count = 0

    for prec_no, pref_name in prefectures:
        records = fetch_station_codes(str(prec_no).zfill(2))
        for rec in records:
            block_no = str(rec.get("block_no", "")).strip()
            if not block_no:
                continue
            item = {
                "prec_no": str(prec_no).zfill(2),
                "pref_name": pref_name,
                "block_no": block_no,
                "station_name": str(rec.get("station", "")),
                "obs_method": str(rec.get("obs_method", "")),
                "obs_type": _to_obs_type(str(rec.get("obs_method", ""))),
            }
            by_block_no.setdefault(block_no, []).append(item)
            station_count += 1

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prefecture_count": len(prefectures),
        "station_count": station_count,
        "by_block_no": by_block_no,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path


def main() -> int:
    path = build_jma_station_index()
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
