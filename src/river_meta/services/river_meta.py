from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from river_meta.models import StationReport
from river_meta.normalizer import CSV_COLUMNS
from river_meta.renderer_csv import render_station_csv_row
from river_meta.renderer_markdown import render_markdown
from river_meta.service import scrape_station


LogFn = Callable[[str], None]


@dataclass(slots=True)
class RiverMetaRunInput:
    station_ids: list[str] | None = None
    id_file: str | None = None
    output_path: str | None = None
    output_dir_md: str | None = None
    output_csv_path: str | None = None
    kinds: tuple[int, ...] = (2, 3)
    page_scan_max: int = 2
    timeout_sec: float = 10
    user_agent: str | None = None
    request_interval_ms: int = 0
    csv_encoding: str = "utf-8-sig"
    csv_delimiter: str = ","
    debug_save_html: str | None = None


@dataclass(slots=True)
class RiverMetaRunResult:
    exit_code: int
    station_count: int
    success_count: int
    failed_count: int
    csv_path: str | None
    csv_row_count: int
    single_markdown: str | None = None


def _read_text_with_fallback(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def _is_header_like(value: str) -> bool:
    lowered = value.lower()
    return lowered in {
        "id",
        "station_id",
        "stationid",
        "obs_id",
        "observation_id",
        "観測所記号",
        "観測所id",
    }


def _read_station_ids_from_txt(path: Path) -> list[str]:
    lines = _read_text_with_fallback(path).splitlines()
    ids: list[str] = []
    for line in lines:
        value = line.strip()
        if not value or value.startswith("#"):
            continue
        ids.append(value)
    return ids


def _read_station_ids_from_csv(path: Path) -> list[str]:
    content = _read_text_with_fallback(path)
    reader = csv.reader(content.splitlines())
    ids: list[str] = []
    for index, row in enumerate(reader):
        if not row:
            continue
        value = row[0].strip()
        if not value or value.startswith("#"):
            continue
        if index == 0 and _is_header_like(value):
            continue
        ids.append(value)
    return ids


def _read_station_ids_from_file(path: str) -> list[str]:
    file_path = Path(path)
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return _read_station_ids_from_csv(file_path)
    return _read_station_ids_from_txt(file_path)


def collect_station_ids(
    station_ids: list[str] | None,
    id_file: str | None,
) -> list[str]:
    values: list[str] = []
    if station_ids:
        values.extend(station_ids)
    if id_file:
        values.extend(_read_station_ids_from_file(id_file))

    normalized = [value.strip() for value in values if value and value.strip()]
    unique: list[str] = []
    seen: set[str] = set()
    for station_id in normalized:
        if station_id in seen:
            continue
        seen.add(station_id)
        unique.append(station_id)
    return unique


def _default_csv_filename(station_count: int) -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"station_summary_{timestamp}_N{station_count}.csv"


def _resolve_csv_output_path(raw_path: str | None, station_count: int) -> str | None:
    if not raw_path:
        if station_count <= 1:
            return None
        output_dir = Path("out")
        output_dir.mkdir(parents=True, exist_ok=True)
        return str(output_dir / _default_csv_filename(station_count))

    path = Path(raw_path)
    is_dir_hint = raw_path.endswith("/") or raw_path.endswith("\\")
    if (path.exists() and path.is_dir()) or is_dir_hint:
        path.mkdir(parents=True, exist_ok=True)
        return str(path / _default_csv_filename(station_count))
    return str(path)


def _write_markdown_per_station(content: str, output_dir: str, station_id: str) -> None:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    filename = f"station_{station_id}.md"
    (directory / filename).write_text(content, encoding="utf-8")


def _status_from_report(report: StationReport) -> str:
    has_error = any(log.level == "ERROR" for log in report.logs)
    has_warn = any(log.level == "WARN" for log in report.logs)
    if has_error:
        return "partial"
    if has_warn:
        return "warn"
    return "ok"


def _render_failed_csv_row(station_id: str, message: str) -> dict[str, str]:
    row = {column: "" for column in CSV_COLUMNS}
    row["station_id"] = station_id
    row["status"] = "failed"
    row["error_count"] = "1"
    row["warn_count"] = "0"
    row["error_message"] = message
    return row


def _add_status_columns(
    row: dict[str, str],
    *,
    status: str,
    error_count: int,
    warn_count: int,
    error_message: str = "",
) -> dict[str, str]:
    row = dict(row)
    row["status"] = status
    row["error_count"] = str(error_count)
    row["warn_count"] = str(warn_count)
    row["error_message"] = error_message
    return row


def _open_csv_writer(path: str, *, encoding: str, delimiter: str) -> tuple[object, csv.DictWriter]:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    file = output_path.open("w", encoding=encoding, newline="")
    writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS, delimiter=delimiter)
    writer.writeheader()
    return file, writer


def _write_csv_row(writer: csv.DictWriter, row: dict[str, str]) -> None:
    writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})


def _noop_log(_: str) -> None:
    return None


def run_river_meta(
    config: RiverMetaRunInput,
    *,
    scrape_station_fn: Callable[..., StationReport] = scrape_station,
    log_info: LogFn | None = None,
    log_warn: LogFn | None = None,
    log_error: LogFn | None = None,
) -> RiverMetaRunResult:
    warn = log_warn or _noop_log
    error = log_error or _noop_log

    station_ids = collect_station_ids(config.station_ids, config.id_file)
    if not station_ids:
        raise ValueError("--id or --id-file is required.")

    is_multi = len(station_ids) > 1
    csv_path = _resolve_csv_output_path(config.output_csv_path, len(station_ids))
    failed_count = 0
    success_count = 0
    csv_row_count = 0
    csv_file = None
    csv_writer: csv.DictWriter | None = None
    single_markdown: str | None = None

    if is_multi and not config.output_dir_md:
        warn(
            "[river-meta] markdown output skipped for multiple stations. "
            "Use --out-dir-md to save per-station markdown files."
        )

    if csv_path:
        csv_file, csv_writer = _open_csv_writer(
            csv_path,
            encoding=config.csv_encoding,
            delimiter=config.csv_delimiter,
        )

    try:
        for station_id in station_ids:
            try:
                report = scrape_station_fn(
                    station_id,
                    kinds=tuple(config.kinds),
                    page_scan_max=config.page_scan_max,
                    timeout=config.timeout_sec,
                    user_agent=config.user_agent,
                    request_interval_ms=config.request_interval_ms,
                    debug_save_html_dir=config.debug_save_html,
                )
            except Exception as exc:  # noqa: BLE001
                failed_count += 1
                message = f"{type(exc).__name__}: {exc}"
                error(f"[river-meta] station={station_id} failed: {message}")
                if csv_writer is not None:
                    _write_csv_row(csv_writer, _render_failed_csv_row(station_id, message))
                    csv_row_count += 1
                continue

            success_count += 1
            error_count = sum(1 for event in report.logs if event.level == "ERROR")
            warn_count = sum(1 for event in report.logs if event.level == "WARN")
            status = _status_from_report(report)
            warn(
                "[river-meta] log summary: "
                f"station={station_id} ERROR={error_count}, WARN={warn_count}, TOTAL={len(report.logs)}"
            )

            markdown = render_markdown(report)
            if config.output_dir_md:
                _write_markdown_per_station(markdown, config.output_dir_md, report.station_id)
            elif not is_multi:
                if config.output_path:
                    output_path = Path(config.output_path)
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    output_path.write_text(markdown, encoding="utf-8")
                else:
                    single_markdown = markdown

            if csv_writer is not None:
                row = render_station_csv_row(report)
                _write_csv_row(
                    csv_writer,
                    _add_status_columns(
                        row,
                        status=status,
                        error_count=error_count,
                        warn_count=warn_count,
                    ),
                )
                csv_row_count += 1
    finally:
        if csv_file is not None:
            csv_file.close()

    if success_count == 0 and csv_row_count == 0:
        error("[river-meta] fatal: no station report generated.")
        return RiverMetaRunResult(
            exit_code=2,
            station_count=len(station_ids),
            success_count=success_count,
            failed_count=failed_count,
            csv_path=csv_path,
            csv_row_count=csv_row_count,
            single_markdown=single_markdown,
        )

    if csv_path and csv_row_count > 0:
        warn(f"[river-meta] csv written: {csv_path}")

    return RiverMetaRunResult(
        exit_code=0 if failed_count == 0 else 1,
        station_count=len(station_ids),
        success_count=success_count,
        failed_count=failed_count,
        csv_path=csv_path,
        csv_row_count=csv_row_count,
        single_markdown=single_markdown,
    )
