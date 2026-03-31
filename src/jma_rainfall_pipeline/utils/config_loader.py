"""出力先パスの解決ユーティリティ（設定ファイル非依存）。"""

from pathlib import Path
from typing import Any, Dict

from .path_utils import get_project_root


def get_output_directories(
    config: Dict[str, Any] | None = None,
    csv_dir_override: Path | None = None,
    excel_dir_override: Path | None = None,
    log_file_override: Path | None = None,
) -> Dict[str, str]:
    """Resolve output-related directories with optional overrides."""

    if config is None or not isinstance(config, dict):
        config = {}

    output_config = config.get("output", {}) or {}
    logging_config = config.get("logging", {}) or {}

    project_root = get_project_root()

    def resolve_path(override: Path | None, default: str) -> Path:
        target = override if override is not None else default
        path = Path(target)
        if not path.is_absolute():
            path = project_root / path
        return path

    csv_path = resolve_path(csv_dir_override, output_config.get("csv_dir", "outputs/jma/csv"))
    excel_path = resolve_path(excel_dir_override, output_config.get("excel_dir", "outputs/jma/excel"))
    parquet_path = resolve_path(None, output_config.get("parquet_dir", "outputs/jma/parquet"))
    log_path = resolve_path(log_file_override, logging_config.get("file", "outputs/jma/jma_app.log"))

    return {
        "csv_dir": str(csv_path),
        "excel_dir": str(excel_path),
        "parquet_dir": str(parquet_path),
        "db_url": output_config.get("db_url", "sqlite:///data/precip.db"),
        "log_file": str(log_path),
    }
