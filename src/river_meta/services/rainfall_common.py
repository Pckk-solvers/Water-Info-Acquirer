from __future__ import annotations

from pathlib import Path
from typing import Callable

LogFn = Callable[[str], None]
CancelFn = Callable[[], bool]


def noop_log(_: str) -> None:
    return None


def append_cancelled_once(errors: list[str]) -> None:
    if "cancelled" not in errors:
        errors.append("cancelled")


def rollback_created_parquets(paths: list[Path], logger: LogFn) -> None:
    if not paths:
        return
    unique_paths = list(dict.fromkeys(paths))
    removed = 0
    for path in unique_paths:
        try:
            if path.exists():
                path.unlink()
                removed += 1
        except Exception as exc:  # noqa: BLE001
            logger(f"[Parquet][WARN] ロールバック削除失敗: {path.name} ({type(exc).__name__}: {exc})")
    if removed:
        logger(f"[Parquet] 停止により {removed} 件の新規Parquetをロールバック削除しました。")


def sanitize_path_token(value: str) -> str:
    return str(value).replace("/", "_").replace("\\", "_")


def build_excel_output_path(output_dir: str | Path, station_key: str, station_name: str) -> Path:
    safe_name = sanitize_path_token(station_name)
    filename = f"{station_key}_{safe_name}.xlsx" if safe_name else f"{station_key}.xlsx"
    return Path(output_dir) / "excel" / filename


def build_chart_output_path(
    output_dir: str | Path,
    station_key: str,
    station_name: str,
    year: int,
    metric: str,
) -> Path:
    safe_station_name = sanitize_path_token(station_name) if station_name else ""
    safe_station_key = sanitize_path_token(station_key)
    subdir_name = f"{safe_station_name}_{safe_station_key}" if safe_station_name else safe_station_key
    safe_metric = sanitize_path_token(metric)
    return Path(output_dir) / "charts" / subdir_name / f"{year}_{safe_metric}.png"


def to_relpath(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def is_cancelled(should_stop: CancelFn | None) -> bool:
    if should_stop is None:
        return False
    try:
        return bool(should_stop())
    except Exception:
        return False
