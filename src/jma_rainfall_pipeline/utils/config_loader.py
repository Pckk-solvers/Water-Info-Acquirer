"""設定ファイル読み込みユーティリティ"""

from pathlib import Path
from typing import Any, Dict

import yaml

from ..logger.app_logger import get_logger
from .path_utils import get_project_root


logger = get_logger(__name__)


def get_default_config_path() -> Path:
    """Return the canonical config file location."""

    current_dir = Path(__file__).parent.parent
    return current_dir / "config.yml"


def config_file_exists(config_path: str | Path | None = None) -> bool:
    """Return True if the configuration file exists."""

    path = Path(config_path) if config_path is not None else get_default_config_path()
    return path.exists()


def load_config(config_path: str | Path | None = None) -> Dict[str, Any]:
    """Load configuration data, tolerating missing files."""

    if config_path is None:
        config_path = get_default_config_path()
    else:
        config_path = Path(config_path)

    try:
        with config_path.open("r", encoding="utf-8") as file:
            config = yaml.safe_load(file) or {}
        logger.info("設定ファイルを読み込みました: %s", config_path)
        return config
    except FileNotFoundError:
        logger.warning("設定ファイルが見つかりません。デフォルト設定を使用します: %s", config_path)
        return {}
    except yaml.YAMLError as exc:
        logger.error("設定ファイルの解析エラー: %s", exc)
        raise
    except Exception as exc:  # pragma: no cover - unexpected I/O errors
        logger.error("設定ファイル読み込みエラー: %s", exc)
        raise


def get_output_directories(
    config: Dict[str, Any] | None = None,
    csv_dir_override: Path | None = None,
    excel_dir_override: Path | None = None,
    log_file_override: Path | None = None,
) -> Dict[str, str]:
    """Resolve output-related directories with optional overrides."""

    if config is None:
        config = load_config()
    if not isinstance(config, dict):
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

    csv_path = resolve_path(csv_dir_override, output_config.get("csv_dir", "jma_rainfall/csv"))
    excel_path = resolve_path(excel_dir_override, output_config.get("excel_dir", "jma_rainfall/excel"))
    log_path = resolve_path(log_file_override, logging_config.get("file", "jma_rainfall/logs/app.log"))

    return {
        "csv_dir": str(csv_path),
        "excel_dir": str(excel_path),
        "db_url": output_config.get("db_url", "sqlite:///data/precip.db"),
        "log_file": str(log_path),
    }
