# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pdfplumber==0.11.4",
# ]
# ///

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path

# srcディレクトリをパスに追加して river_meta モジュールをインポート可能にする
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


DEFAULT_PDF_PATH = Path("data/source/amedas/ame_master.pdf")
DEFAULT_INDEX_PATH = Path("src/river_meta/resources/jma_station_index.json")


@dataclass(slots=True)
class MergeSummary:
    updated_exact: int = 0
    updated_fuzzy_pref: int = 0
    updated_name_only: int = 0
    preserved_existing: int = 0
    not_found_count: int = 0

    @property
    def updated_total(self) -> int:
        return self.updated_exact + self.updated_fuzzy_pref + self.updated_name_only


def _normalize_pref_name(value: str) -> str:
    text = (value or "").replace("\u3000", " ").strip()
    text = "".join(text.split())
    if text.endswith(("都", "府", "県")):
        text = text[:-1]
    return text


_PDF_FIELDS = (
    "station_id",
    "latitude",
    "longitude",
    "elevation_m",
    "start_date_raw",
    "start_date",
)

_START_DATE_RE = re.compile(
    r"^(?:(?P<era>[明大昭平令])(?P<era_year>元|\d{1,3})|(?P<western>\d{4}))"
    r"[./-](?P<month>\d{1,2})[./-](?P<day>\d{1,2})$"
)

_ERA_BASE_YEAR = {
    "明": 1867,
    "大": 1911,
    "昭": 1925,
    "平": 1988,
    "令": 2018,
}


def _normalize_start_date(value: str) -> str:
    raw = str(value or "").strip().lstrip("#")
    if not raw:
        return ""
    translated = raw.translate(
        str.maketrans(
            "０１２３４５６７８９．／－",
            "0123456789./-",
        )
    )
    matched = _START_DATE_RE.fullmatch(translated)
    if not matched:
        return ""

    year: int
    if matched.group("western"):
        year = int(matched.group("western"))
    else:
        era = matched.group("era") or ""
        era_year_token = matched.group("era_year") or ""
        era_year = 1 if era_year_token == "元" else int(era_year_token)
        base = _ERA_BASE_YEAR.get(era)
        if base is None:
            return ""
        year = base + era_year

    month = int(matched.group("month"))
    day = int(matched.group("day"))
    try:
        return datetime(year, month, day).date().isoformat()
    except ValueError:
        return ""


def _station_lookup_key(station: dict) -> tuple[str, str, str]:
    return (
        str(station.get("prec_no", "")).zfill(2),
        str(station.get("block_no", "")).strip(),
        str(station.get("obs_type", "a1")).strip().lower() or "a1",
    )


def _build_station_lookup(index_data: dict) -> dict[tuple[str, str, str], dict]:
    lookup: dict[tuple[str, str, str], dict] = {}
    for stations in index_data.get("by_block_no", {}).values():
        for station in stations:
            lookup[_station_lookup_key(station)] = station
    return lookup


def _apply_pdf_fields(station: dict, matched_row: dict[str, str]) -> None:
    raw_start_date = str(matched_row.get("start_date", "") or "").strip()
    station["station_id"] = str(matched_row.get("station_id", "") or "").strip()
    station["latitude"] = str(matched_row.get("latitude", "") or "").strip()
    station["longitude"] = str(matched_row.get("longitude", "") or "").strip()
    station["elevation_m"] = str(matched_row.get("elevation_m", "") or "").strip()
    station["start_date_raw"] = raw_start_date
    station["start_date"] = _normalize_start_date(raw_start_date)


def _copy_existing_pdf_fields(station: dict, existing_station: dict | None) -> bool:
    if not existing_station:
        return False
    copied = False
    for field_name in _PDF_FIELDS:
        value = str(existing_station.get(field_name, "") or "").strip()
        station[field_name] = value
        copied = copied or bool(value)
    return copied


def _clear_pdf_fields(station: dict) -> None:
    for field_name in _PDF_FIELDS:
        station[field_name] = ""


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JMA観測所インデックスをPDF情報で補完更新する (update/rebuild 対応)"
    )
    parser.add_argument(
        "--mode",
        choices=["update", "rebuild"],
        default="update",
        help="update: 既存JSONに補完を適用 / rebuild: ベースJSONを再生成してから補完を適用",
    )
    parser.add_argument(
        "--in-pdf",
        dest="in_pdf",
        default=str(DEFAULT_PDF_PATH),
        help=f"AMeDASマスタPDFパス (既定: {DEFAULT_PDF_PATH})",
    )
    parser.add_argument(
        "--index",
        default=str(DEFAULT_INDEX_PATH),
        help=f"入力JSONパス (update時のベース, 既定: {DEFAULT_INDEX_PATH})",
    )
    parser.add_argument(
        "--output",
        default="",
        help="出力JSONパス (未指定時は --index と同じ)",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="出力先上書き時の .bak 生成を行わない",
    )
    return parser.parse_args(argv)


def _build_lookup(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    lookup_name: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        pref = _normalize_pref_name(row.get("prefecture", "")).replace("地方", "")
        name = row.get("station_name", "")
        if not name:
            continue
        lookup_name.setdefault(name, []).append({"pref": pref, "data": row})
    return lookup_name


def _merge_pdf_data(
    index_data: dict,
    lookup_name: dict[str, list[dict[str, str]]],
    *,
    existing_lookup: dict[tuple[str, str, str], dict] | None = None,
) -> MergeSummary:
    summary = MergeSummary()
    existing_lookup = existing_lookup or {}

    for _block_no, stations in index_data.get("by_block_no", {}).items():
        for station in stations:
            api_pref = _normalize_pref_name(station.get("pref_name", "")).replace("地方", "")
            name = station.get("station_name", "")
            station_key = _station_lookup_key(station)

            match: dict[str, str] | None = None
            match_type = ""
            candidates = lookup_name.get(name, [])

            if candidates:
                for cand in candidates:
                    pdf_pref = cand["pref"]
                    if api_pref == pdf_pref:
                        match = cand["data"]
                        match_type = "exact"
                        break
                    if api_pref in pdf_pref or pdf_pref in api_pref:
                        match = cand["data"]
                        match_type = "fuzzy_pref"
                        break
                    # 北海道特例: APIは「北海道」だが、PDFは地方区分の場合がある
                    if api_pref == "北海道" and pdf_pref not in ("青森", "秋田", "岩手"):
                        match = cand["data"]
                        match_type = "fuzzy_pref"
                        break

                if not match and len(candidates) == 1:
                    match = candidates[0]["data"]
                    match_type = "name_only"
                elif not match and len(candidates) > 1:
                    pref_list = [c["pref"] for c in candidates]
                    print(
                        f"[SKIP] 名前が重複し地域一致しないためスキップ: {api_pref} - {name} "
                        f"(候補: {pref_list})"
                    )

            if match:
                _apply_pdf_fields(station, match)
                if match_type == "exact":
                    summary.updated_exact += 1
                elif match_type == "fuzzy_pref":
                    summary.updated_fuzzy_pref += 1
                else:
                    summary.updated_name_only += 1
            else:
                existing_station = existing_lookup.get(station_key)
                if _copy_existing_pdf_fields(station, existing_station):
                    summary.preserved_existing += 1
                else:
                    _clear_pdf_fields(station)
                    summary.not_found_count += 1
                    if not candidates:
                        print(f"[WARN] 見つかりません: {api_pref} - {name}")

    return summary


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _backup_if_needed(path: Path, enabled: bool) -> Path | None:
    if not enabled or not path.exists():
        return None
    backup = Path(str(path) + ".bak")
    shutil.copy2(path, backup)
    return backup


def run_update_jma_index(
    *,
    mode: str,
    pdf_path: Path,
    index_path: Path,
    output_path: Path,
    backup_enabled: bool,
) -> int:
    try:
        from river_meta.amedas.extract import extract_amedas_table_rows
    except ModuleNotFoundError as exc:
        print(
            "Error: PDF解析依存が不足しています。"
            " `uv run scripts/update_jma_station_index.py ...` で実行してください。"
        )
        print(f"詳細: {exc}")
        return 1

    from river_meta.rainfall.build_station_index import build_jma_station_index

    if not pdf_path.exists():
        print(f"Error: PDFが見つかりません: {pdf_path}")
        return 1

    print("--- 1. PDFから観測所データを抽出 ---")
    rows, stats = extract_amedas_table_rows(in_pdf=str(pdf_path))
    print(f"PDFパース完了: {len(rows)}件取得 (全{stats.total_rows}行中 / {stats.skipped_rows}行スキップ)")
    lookup_name = _build_lookup(rows)

    print("\n--- 2. ベースJSONの読み込み ---")
    base_tmp_path: Path | None = None
    existing_lookup: dict[tuple[str, str, str], dict] = {}
    if index_path.exists():
        existing_lookup = _build_station_lookup(_load_json(index_path))
    if mode == "rebuild":
        base_tmp_path = output_path.with_suffix(output_path.suffix + ".base.tmp")
        print(f"モード: rebuild -> JMAベースJSONを再生成: {base_tmp_path}")
        build_jma_station_index(output_path=str(base_tmp_path))
        data = _load_json(base_tmp_path)
    else:
        print(f"モード: update -> 既存JSONを使用: {index_path}")
        if not index_path.exists():
            print(f"Error: JSONが見つかりません: {index_path}")
            return 1
        data = _load_json(index_path)

    print("\n--- 3. JSONとPDFデータのマージ ---")
    summary = _merge_pdf_data(data, lookup_name, existing_lookup=existing_lookup)
    print("マージ結果:")
    print(f"  - 完全一致(都道府県+観測所名): {summary.updated_exact}件")
    print(f"  - 部分一致(都道府県揺れ許容) : {summary.updated_fuzzy_pref}件")
    print(f"  - 観測所名のみ一致           : {summary.updated_name_only}件")
    print(f"  - 合計更新件数               : {summary.updated_total}件")
    if summary.preserved_existing > 0:
        print(f"  - 既存値フォールバック維持   : {summary.preserved_existing}件")
    if summary.not_found_count > 0:
        print(
            f"  - PDFにも既存値にも見つからず未補完: {summary.not_found_count}件 "
            "(station_id/座標/標高/開始日は空文字)"
        )

    print("\n--- 4. 保存 ---")
    backup_path = _backup_if_needed(output_path, backup_enabled)
    if backup_path:
        print(f"Backup: {backup_path}")
    _save_json(output_path, data)
    print(f"完了: {output_path}")

    if base_tmp_path and base_tmp_path.exists():
        base_tmp_path.unlink(missing_ok=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    index_path = Path(args.index)
    output_path = Path(args.output) if args.output else index_path
    return run_update_jma_index(
        mode=args.mode,
        pdf_path=Path(args.in_pdf),
        index_path=index_path,
        output_path=output_path,
        backup_enabled=not args.no_backup,
    )


if __name__ == "__main__":
    raise SystemExit(main())
