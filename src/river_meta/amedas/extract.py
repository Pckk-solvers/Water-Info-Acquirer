from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pdfplumber


STATION_ID_RE = re.compile(r"^\d{5}$")
NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")


def normalize_pref_name(value: str) -> str:
    text = (value or "").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", "", text)
    if text.endswith(("都", "府", "県")):
        text = text[:-1]
    return text


def _safe_float(text: str) -> float | None:
    match = NUMBER_RE.search(text or "")
    if not match:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


def _to_decimal(deg_text: str, min_text: str) -> float | None:
    deg = _safe_float(deg_text)
    minute = _safe_float(min_text)
    if deg is None or minute is None:
        return None
    return deg + minute / 60.0


@dataclass(slots=True)
class ExtractStats:
    total_rows: int
    matched_rows: int
    skipped_rows: int


def _to_str(value: object) -> str:
    return str(value).strip() if value is not None else ""


def extract_amedas_table_rows(
    *,
    in_pdf: str,
    pref_names: set[str] | None = None,
) -> tuple[list[dict[str, str]], ExtractStats]:
    path = Path(in_pdf)
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {in_pdf}")

    normalized_prefs = {normalize_pref_name(name) for name in pref_names} if pref_names else None

    rows: list[dict[str, str]] = []
    total_rows = 0
    matched_rows = 0
    skipped_rows = 0

    with pdfplumber.open(path) as pdf:
        for page_idx, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables() or []
            for table in tables:
                current_pref = ""
                for raw_row in table:
                    if not raw_row:
                        continue
                    cols = [_to_str(cell) for cell in raw_row]
                    if len(cols) < 17:
                        continue
                    total_rows += 1

                    station_id = cols[1]
                    if not STATION_ID_RE.fullmatch(station_id):
                        if cols[0] and "管理" not in cols[0]:
                            current_pref = cols[0]
                        skipped_rows += 1
                        continue

                    pref = cols[0] or current_pref
                    current_pref = pref or current_pref
                    pref_norm = normalize_pref_name(pref)
                    if normalized_prefs is not None and pref_norm not in normalized_prefs:
                        skipped_rows += 1
                        continue

                    lat_dec = _to_decimal(cols[7], cols[8])
                    lon_dec = _to_decimal(cols[9], cols[10])
                    latitude = f"{lat_dec:.6f}" if lat_dec is not None else ""
                    longitude = f"{lon_dec:.6f}" if lon_dec is not None else ""

                    row = {
                        "station_id": station_id,
                        "prefecture": pref,
                        "station_type": cols[2],
                        "station_name": cols[3],
                        "station_name_kana": cols[4],
                        "display_name": cols[5],
                        "location": cols[6],
                        "latitude": latitude,
                        "longitude": longitude,
                        "lat_deg": cols[7],
                        "lat_min": cols[8],
                        "lon_deg": cols[9],
                        "lon_min": cols[10],
                        "elevation_m": cols[11],
                        "wind_height_m": cols[12],
                        "temp_height_m": cols[13],
                        "start_date": cols[14],
                        "note1": cols[15],
                        "note2": cols[16],
                        "source_page": str(page_idx),
                    }
                    rows.append(row)
                    matched_rows += 1

    stats = ExtractStats(
        total_rows=total_rows,
        matched_rows=matched_rows,
        skipped_rows=skipped_rows,
    )
    return rows, stats
