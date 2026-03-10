from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from river_meta.amedas.extract import extract_amedas_table_rows, normalize_pref_name


DEFAULT_PREFS = ["兵庫", "大阪", "京都", "奈良", "和歌山"]
DEFAULT_PDF_PATH = "data/source/amedas/ame_master.pdf"
LEGACY_PDF_PATH = "ame_master.pdf"

LogFn = Callable[[str], None]


@dataclass(slots=True)
class AmedasRunInput:
    in_pdf: str = DEFAULT_PDF_PATH
    out_csv: str = "data/out/ame_master_kinki.csv"
    pref: list[str] = field(default_factory=list)
    pref_list: str = ""
    all_pref: bool = False
    encoding: str = "utf-8-sig"


@dataclass(slots=True)
class AmedasRunResult:
    output_csv: str
    rows: int
    total_rows: int
    skipped_rows: int


def _noop_log(_: str) -> None:
    return None


def _parse_pref_names(pref: list[str], pref_list: str, all_pref: bool) -> set[str] | None:
    if all_pref:
        return None
    names = list(pref)
    names.extend([item.strip() for item in pref_list.split(",") if item.strip()])
    if not names:
        names = list(DEFAULT_PREFS)
    return {normalize_pref_name(name) for name in names if normalize_pref_name(name)}


def _write_rows(path: str, rows: list[dict[str, str]], *, encoding: str) -> None:
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "station_id",
        "prefecture",
        "station_type",
        "station_name",
        "station_name_kana",
        "display_name",
        "location",
        "latitude",
        "longitude",
        "lat_deg",
        "lat_min",
        "lon_deg",
        "lon_min",
        "start_date",
        "source_page",
    ]
    with out_path.open("w", encoding=encoding, newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _resolve_input_pdf_path(path: str) -> str:
    candidate = Path(path)
    if candidate.exists():
        return str(candidate)
    if path == DEFAULT_PDF_PATH and Path(LEGACY_PDF_PATH).exists():
        return LEGACY_PDF_PATH
    return str(candidate)


def run_amedas_extract(config: AmedasRunInput, *, log: LogFn | None = None) -> AmedasRunResult:
    logger = log or _noop_log
    pref_names = _parse_pref_names(config.pref, config.pref_list, config.all_pref)
    in_pdf = _resolve_input_pdf_path(config.in_pdf)

    rows, stats = extract_amedas_table_rows(in_pdf=in_pdf, pref_names=pref_names)
    _write_rows(config.out_csv, rows, encoding=config.encoding)

    logger(
        "[river-ame-master] done: "
        f"rows={len(rows)}, total_rows={stats.total_rows}, skipped={stats.skipped_rows}, out={config.out_csv}"
    )
    return AmedasRunResult(
        output_csv=config.out_csv,
        rows=len(rows),
        total_rows=stats.total_rows,
        skipped_rows=stats.skipped_rows,
    )
