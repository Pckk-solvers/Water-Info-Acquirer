# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "beautifulsoup4",
#     "requests",
# ]
# ///

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import bs4
import requests

# 既存のriver_metaをシステムパスに追加して利用可能にする
src_dir = Path(__file__).resolve().parents[1] / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from river_meta.station_ids.core import (
    DEFAULT_UA,
    build_session,
    collect_station_ids,
    fetch_master_options,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

INDEX_FILE_PATH = src_dir / "river_meta" / "resources" / "waterinfo_station_index.json"


def fetch_site_info(session: requests.Session, station_id: str, timeout: float) -> dict[str, str]:
    url = f"https://www1.river.go.jp/cgi-bin/SiteInfo.exe?ID={station_id}"
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    html = resp.text

    soup = bs4.BeautifulSoup(html, "html.parser")
    results = {}
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            key = tds[0].get_text(strip=True)
            val = tds[1].get_text(strip=True)
            if key and key not in results:
                results[key] = val
    return results


def parse_metadata(station_id: str, raw_data: dict[str, str], pref_code_map: dict[str, str], pref_name: str) -> dict:
    # key (e.g., 観測所名, 水系名, 河川名, 所在地, 緯度経度)
    
    # 観測所名の抽出（ふりがな含む）: 例 "和歌山（わかやま）"
    raw_name = raw_data.get("観測所名", "")
    name = raw_name
    name_kana = ""
    match = re.match(r"^(.+?)[\(（](.+?)[\)）]$", raw_name)
    if match:
        name = match.group(1).strip()
        name_kana = match.group(2).strip()

    # 水系・河川
    suikei_name = raw_data.get("水系名", "").replace("（わかやま）", "") # In some places it grabs weird stuff, just clean if needed
    kasen_name = raw_data.get("河川名", "")

    # 緯度・経度
    # 例: 北緯 34度13分18秒 東経 135度09分50秒
    raw_lat_lon = raw_data.get("緯度経度", "")
    latitude = ""
    longitude = ""
    lat_match = re.search(r"北緯\s*(\d+)度(\d+)分(\d+)秒", raw_lat_lon)
    if lat_match:
        deg, min_, sec = map(int, lat_match.groups())
        lat_dec = deg + min_ / 60.0 + sec / 3600.0
        latitude = f"{lat_dec:.6f}"
        
    lon_match = re.search(r"東経\s*(\d+)度(\d+)分(\d+)秒", raw_lat_lon)
    if lon_match:
        deg, min_, sec = map(int, lon_match.groups())
        lon_dec = deg + min_ / 60.0 + sec / 3600.0
        longitude = f"{lon_dec:.6f}"

    if not name and not latitude and not longitude:
        raise ValueError("Empty site data (missing station name and coordinates)")

    return {
        "station_id": station_id,
        "station_name": name,
        "station_name_kana": name_kana,
        "pref_code": next((k for k, v in pref_code_map.items() if v == pref_name), ""),
        "pref_name": pref_name,
        "suikei_name": suikei_name,
        "kasen_name": kasen_name,
        "location": raw_data.get("所在地", ""),
        "latitude": latitude,
        "longitude": longitude,
        "items": ["雨量"], # 今回は雨量（KOMOKU=01）で固定検索するため
    }


def main():
    parser = argparse.ArgumentParser(description="build waterinfo station index for rainfall")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP Timeout")
    parser.add_argument("--sleep", type=float, default=0.3, help="Sleep between requests")
    parser.add_argument("--max-count", type=int, default=0, help="Max stations to process for testing")
    parser.add_argument("--test-pref", type=str, default="", help="Only fetch IDs for this prefecture code (for testing)")
    args = parser.parse_args()

    session = build_session(user_agent=DEFAULT_UA)

    # 1. 既存または新規JSONの読み込み（再開用）
    current_data = {}
    if INDEX_FILE_PATH.exists():
        logger.info(f"Loading existing index: {INDEX_FILE_PATH}")
        with open(INDEX_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if "by_station_id" in data:
                current_data = data["by_station_id"]
    
    # 2. マスターから都道府県一覧を取得
    logger.info("Fetching prefecture master data...")
    try:
        pref_options, _ = fetch_master_options(session, timeout=args.timeout)
    except Exception as e:
        logger.error(f"Failed to fetch master data: {e}")
        return 1

    pref_code_map = {code: name for code, name in pref_options if code != "-1"}
    
    # 3. 各都道府県ごとのIDを取得 または抽出（1度だけでいいが全件更新の場合）
    # KOMOKU=01 (雨量)
    logger.info("Collecting rainfall station IDs across prefectures...")
    
    all_targets = []
    
    base_params = {
        "CITY": "",
        "KASEN": "",
        "KOMOKU": "01",
        "NAME": "",
        "SUIKEI": "-00001",
    }
    
    prefs_to_check = pref_code_map.items()
    if args.test_pref:
        prefs_to_check = [(code, name) for code, name in prefs_to_check if code == args.test_pref or name == args.test_pref]
        logger.info(f"Limiting to prefecture: {prefs_to_check}")

    for pref_code, pref_name in prefs_to_check:
        params = dict(base_params)
        params["KEN"] = pref_code
        try:
            ids, total = collect_station_ids(
                session,
                params=params,
                timeout=args.timeout,
                sleep_sec=args.sleep,
                page_max=50,
            )
            for sid in ids:
                all_targets.append((sid, pref_name))
            logger.info(f"Collected {len(ids)} stations for {pref_name}")
        except Exception as e:
            logger.warning(f"Failed to collect IDs for {pref_name}: {e}")
            
        time.sleep(args.sleep)

    logger.info(f"Total rainfall stations found: {len(all_targets)}")
    
    # Skip already processed
    targets_to_process = [(sid, pname) for sid, pname in all_targets if sid not in current_data]
    logger.info(f"Stations left to fetch metadata: {len(targets_to_process)}")
    
    if args.max_count > 0:
        targets_to_process = targets_to_process[:args.max_count]
        logger.info(f"Limiting to {args.max_count} stations for testing")

    # 4. SiteInfo.exe で詳細を取得してマージ
    count = 0
    total_to_process = len(targets_to_process)
    
    try:
        for station_id, pref_name in targets_to_process:
            count += 1
            if count % 10 == 0:
                logger.info(f"Processing {count}/{total_to_process} ...")
                
            try:
                raw_data = fetch_site_info(session, station_id, timeout=args.timeout)
                meta = parse_metadata(station_id, raw_data, pref_code_map, pref_name)
                current_data[station_id] = meta
            except Exception as e:
                logger.warning(f"Failed to fetch metadata for {station_id} ({pref_name}): {e}")
                
            time.sleep(args.sleep)
            
            # 定期保存
            if count % 100 == 0:
                _save_json(current_data, INDEX_FILE_PATH, logger)
                
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving current progress...")
    finally:
        _save_json(current_data, INDEX_FILE_PATH, logger)
        
    return 0

def _save_json(data_map: dict, filepath: Path, logger: logging.Logger):
    prefs = set()
    for meta in data_map.values():
        if "pref_name" in meta and meta["pref_name"]:
            prefs.add(meta["pref_name"])
            
    out_obj = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "prefecture_count": len(prefs),
        "station_count": len(data_map),
        "by_station_id": dict(sorted(data_map.items()))
    }
    INDEX_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(INDEX_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(out_obj, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(data_map)} stations to {INDEX_FILE_PATH}")


if __name__ == "__main__":
    sys.exit(main())
