"""アプリ/モジュール名称・バージョンの一元管理。"""

from importlib.metadata import PackageNotFoundError, version as meta_version
from pathlib import Path
import sys
import tomllib

APP = {
    "jp": "水文データ取得ツール",
    "en": "Water Info Acquirer",
}

MODULES = {
    "water_info": {
        "jp": "国土交通省 水文データ取得",
        "en": "Water Info",
    },
    "jma": {
        "jp": "気象庁 雨量データ取得",
        "en": "JMA Rainfall",
    },
    "rainfall": {
        "jp": "雨量整理・抽出",
        "en": "Rainfall Tools",
    },
}

PACKAGE_NAME = "Water-Info-Acquirer"
FALLBACK_VERSION = "0.0.0"


def get_app_title(lang: str = "jp") -> str:
    return APP.get(lang, APP["jp"])


def get_module_title(key: str, lang: str = "jp") -> str:
    return MODULES.get(key, {}).get(lang, key)


def _read_pyproject_version() -> str | None:
    candidates = []
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / "pyproject.toml")
    candidates.append(Path(__file__).resolve().parent.parent.parent / "pyproject.toml")

    for pyproj in candidates:
        if pyproj.is_file():
            try:
                data = tomllib.loads(pyproj.read_text(encoding="utf-8"))
                return data.get("project", {}).get("version")
            except Exception:
                continue
    return None


def get_version() -> str:
    try:
        return meta_version(PACKAGE_NAME)
    except PackageNotFoundError:
        pass
    except Exception:
        pass

    v = _read_pyproject_version()
    if v:
        return v
    return FALLBACK_VERSION
