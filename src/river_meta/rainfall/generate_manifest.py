from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Callable


LogFn = Callable[[str], None]


def empty_manifest() -> dict[str, object]:
    return {
        "version": 1,
        "excel": {},
        "charts": {},
    }


def load_manifest(path: str | Path, *, log: LogFn | None = None) -> dict[str, object]:
    manifest_path = Path(path)
    if not manifest_path.exists():
        return empty_manifest()

    logger = log or (lambda _: None)
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        logger(f"[generate][WARN] manifest 読み込み失敗のため初期化します: {type(exc).__name__}: {exc}")
        return empty_manifest()
    return _normalize_manifest(raw)


def save_manifest(path: str | Path, manifest: dict[str, object]) -> None:
    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    normalized = _normalize_manifest(manifest)
    temp_path = manifest_path.with_suffix(f"{manifest_path.suffix}.tmp")
    temp_path.write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    temp_path.replace(manifest_path)


def build_station_id(source: str, station_key: str) -> str:
    return f"{source}::{station_key}"


def build_chart_id(source: str, station_key: str, year: int, metric: str) -> str:
    return f"{source}::{station_key}::{year}::{metric}"


def build_digest_from_parquet_paths(base_dir: str | Path, parquet_paths: list[Path]) -> str:
    root = Path(base_dir)
    lines: list[str] = []
    for path in sorted(dict.fromkeys(parquet_paths)):
        if not path.exists():
            continue
        stat = path.stat()
        try:
            rel = path.relative_to(root).as_posix()
        except ValueError:
            rel = path.as_posix()
        lines.append(f"{rel}|{stat.st_mtime_ns}|{stat.st_size}")
    return _build_digest(lines)


def build_station_digest(year_digests: dict[int, str]) -> str:
    lines = [
        f"{year}:{digest}"
        for year, digest in sorted(year_digests.items())
        if digest
    ]
    return _build_digest(lines)


def _build_digest(lines: list[str]) -> str:
    payload = "\n".join(sorted(lines))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_manifest(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict):
        return empty_manifest()

    excel_raw = raw.get("excel")
    charts_raw = raw.get("charts")
    excel: dict[str, dict[str, object]] = {}
    charts: dict[str, dict[str, str]] = {}

    if isinstance(excel_raw, dict):
        for station_id, record in excel_raw.items():
            if not isinstance(record, dict):
                continue
            year_digests_raw = record.get("year_digests")
            year_digests: dict[str, str] = {}
            if isinstance(year_digests_raw, dict):
                for year, digest in year_digests_raw.items():
                    year_text = str(year).strip()
                    digest_text = str(digest).strip()
                    if year_text and digest_text:
                        year_digests[year_text] = digest_text
            excel[str(station_id)] = {
                "station_digest": str(record.get("station_digest", "")).strip(),
                "output_relpath": str(record.get("output_relpath", "")).strip(),
                "year_digests": year_digests,
            }

    if isinstance(charts_raw, dict):
        for chart_id, record in charts_raw.items():
            if not isinstance(record, dict):
                continue
            charts[str(chart_id)] = {
                "year_digest": str(record.get("year_digest", "")).strip(),
                "output_relpath": str(record.get("output_relpath", "")).strip(),
            }

    return {
        "version": 1,
        "excel": excel,
        "charts": charts,
    }
