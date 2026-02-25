"""
JMA降雨量パイプラインのログ設定
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from typing import Literal, Optional

import yaml

from ..utils.path_utils import get_project_root

# 設定ファイルの場所を特定
config_path = None
try:
    # パッケージ内からの相対パス
    package_root = Path(__file__).resolve().parents[1]
    CONFIG_FILENAME = 'config.yml'
    config_path = package_root / CONFIG_FILENAME
except Exception:
    # フォールバック
    pass

_initialized = False  # ログ設定が初期化されたかのフラグ
_runtime_level_override: str | None = None
_runtime_enable_log_output: bool = True
_runtime_logger_scope: Literal["root", "jma"] = "jma"


class ConfigError(Exception):
    """設定エラー"""
    pass


def setup_logging(
    config_path_override: Optional[str] = None,
    *,
    log_file_override: Optional[str | Path] = None,
    level_override: Optional[str] = None,
    enable_log_output: bool = True,
    logger_scope: Literal["root", "jma"] = "jma",
) -> None:
    """
    ログ設定を初期化

    Args:
        config_path_override: 設定ファイルのパス（オーバーライド用）
        log_file_override: ログファイルのパス（実行時オーバーライド）
        level_override: ログレベル（DEBUG/INFO/WARNING/ERROR/CRITICAL）
        enable_log_output: ログ出力の有効/無効
        logger_scope: 設定対象（root または jma 名前空間）
    """
    global _initialized

    # 設定ファイルからログ設定を読み込み
    log_config = _load_log_config(config_path_override)
    if log_file_override is not None:
        log_path = Path(log_file_override)
        if not log_path.is_absolute():
            log_path = get_project_root() / log_path
        log_config["file"] = str(log_path)
    if level_override is not None:
        log_config["level"] = _normalize_level(level_override)

    target_logger = logging.getLogger() if logger_scope == "root" else logging.getLogger("jma_rainfall_pipeline")
    target_logger.setLevel(getattr(logging, log_config["level"]))

    # 既存のハンドラーをクリア
    target_logger.handlers.clear()
    if logger_scope != "root":
        target_logger.propagate = False

    # フォーマッタを作成
    formatter = logging.Formatter(log_config["format"])

    if enable_log_output:
        # ログディレクトリを作成
        log_dir = Path(log_config["file"]).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        # コンソールハンドラー
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        console_handler.setLevel(logging.WARNING)  # コンソールにはWARNING以上のみ
        target_logger.addHandler(console_handler)

        # ローテーションファイルハンドラー
        file_handler = logging.handlers.RotatingFileHandler(
            log_config["file"],
            maxBytes=log_config["max_size_mb"] * 1024 * 1024,  # MBをバイトに変換
            backupCount=log_config["backup_count"],
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        file_handler.setLevel(getattr(logging, log_config["level"]))
        target_logger.addHandler(file_handler)
    else:
        target_logger.addHandler(logging.NullHandler())

    _initialized = True


def set_runtime_log_options(
    *,
    level: str | None = None,
    enable_log_output: bool | None = None,
    logger_scope: Literal["root", "jma"] | None = None,
) -> None:
    """次回の遅延初期化時に使うランタイムログオプションを更新する。"""

    global _runtime_level_override, _runtime_enable_log_output, _runtime_logger_scope, _initialized
    if level is not None:
        _runtime_level_override = _normalize_level(level)
    if enable_log_output is not None:
        _runtime_enable_log_output = bool(enable_log_output)
    if logger_scope is not None:
        _runtime_logger_scope = logger_scope
    _initialized = False


def get_logger(name: str) -> logging.Logger:
    """
    名前付きロガーを取得

    Args:
        name: ロガー名

    Returns:
        Logger: ロガーインスタンス
    """
    global _initialized
    if not _initialized:
        try:
            setup_logging(
                level_override=_runtime_level_override,
                enable_log_output=_runtime_enable_log_output,
                logger_scope=_runtime_logger_scope,
            )
        except Exception as exc:  # pragma: no cover - setup失敗時は標準設定
            logging.basicConfig(level=logging.INFO)
            _initialized = True
            print(f"警告: ログ設定の初期化に失敗しました: {exc}", file=sys.stderr)
    return logging.getLogger(name)


def _normalize_level(level: str) -> str:
    normalized = str(level).strip().upper()
    allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if normalized not in allowed:
        raise ConfigError(f"不正なログレベルです: {level}")
    return normalized


def _load_log_config(config_path_override: Optional[str] = None) -> dict:
    """
    ログ設定を読み込み

    Args:
        config_path_override: 設定ファイルのパス（オーバーライド用）

    Returns:
        dict: ログ設定

    Raises:
        ConfigError: 設定読み込みエラー
    """
    # config_pathはモジュールレベルで定義済み
    actual_config_path = config_path_override or config_path

    if actual_config_path is None or not Path(actual_config_path).exists():
        # デフォルト設定（config_loader と合わせて outputs/jma 配下に統一）
        default_path = get_project_root() / 'outputs' / 'jma' / 'jma_app.log'
        return {
            'level': 'INFO',
            'file': str(default_path),
            'max_size_mb': 10,
            'backup_count': 5,
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        }

    try:
        with open(actual_config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f) or {}

        log_config = config.get('logging', {})

        log_file = log_config.get('file', 'outputs/jma/jma_app.log')
        log_path = Path(log_file)
        if not log_path.is_absolute():
            log_path = get_project_root() / log_file

        return {
            'level': log_config.get('level', 'INFO'),
            'file': str(log_path),
            'max_size_mb': log_config.get('max_size_mb', 10),
            'backup_count': log_config.get('backup_count', 5),
            'format': log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        }
    except Exception as e:
        raise ConfigError(f"ログ設定の読み込みに失敗しました: {e}")


# 初期化は遅延実行（get_logger 内で実施）
