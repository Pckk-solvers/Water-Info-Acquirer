
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

DISABLE_CACHE_ENV = "RIVER_RAINFALL_DISABLE_JMA_CACHE"
DEFAULT_CACHE_ENABLED = True
DEFAULT_STATIONS_CACHE_TTL_HOURS = 168
DEFAULT_STATIONS_REFRESH_ON_START = False
DEFAULT_CACHE_DIR_NAME = "cache"
CACHE_README_FILENAME = "README.txt"
CACHE_README_TEXT = (
    "このディレクトリは、気象庁の都道府県/観測所JSONを一時保存するキャッシュです。\n"
    "削除しても実行時に再生成されます。\n"
)


@dataclass
class CacheEntry:
    data: Any
    expired: bool


class CacheManager:
    """Handle on-disk caching of JMA code lookups."""

    def __init__(self) -> None:
        project_root = Path(__file__).resolve().parents[3]
        self._cache_dir = project_root / 'outputs' / 'jma' / DEFAULT_CACHE_DIR_NAME
        self._station_dir = self._cache_dir / 'stations'

        disabled_by_env = _is_enabled_env(DISABLE_CACHE_ENV)
        self._cache_enabled = DEFAULT_CACHE_ENABLED and not disabled_by_env
        self._ttl_hours = int(DEFAULT_STATIONS_CACHE_TTL_HOURS)
        self._refresh_on_start = DEFAULT_STATIONS_REFRESH_ON_START
        self._refresh_consumed = False
        self._pref_cache = self._cache_dir / 'prefectures.json'
        if self._cache_enabled:
            self._cache_dir.mkdir(parents=True, exist_ok=True)
            self._station_dir.mkdir(parents=True, exist_ok=True)
            self._ensure_cache_readme()

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    @property
    def enabled(self) -> bool:
        return self._cache_enabled

    def load_prefectures(self) -> Optional[CacheEntry]:
        return self._load_entry(self._pref_cache)

    def save_prefectures(self, data: Any) -> None:
        self._save_entry(self._pref_cache, data)

    def load_stations(self, prec_no: str) -> Optional[CacheEntry]:
        filename = f'stations_{str(prec_no).zfill(2)}.json'
        return self._load_entry(self._station_dir / filename)

    def save_stations(self, prec_no: str, data: Any) -> None:
        filename = f'stations_{str(prec_no).zfill(2)}.json'
        self._save_entry(self._station_dir / filename, data)

    def get_data(self, key: str) -> Optional[Any]:
        """キーに紐づくキャッシュデータを取得"""
        path = self._key_to_path(key)
        entry = self._load_entry(path)
        return entry.data if entry and not entry.expired else None

    def set_data(self, key: str, data: Any, ttl_seconds: Optional[int] = None) -> None:
        """キーに紐づくキャッシュデータを保存"""
        path = self._key_to_path(key)
        self._save_entry(path, data, ttl_override=ttl_seconds)

    def invalidate_pattern(self, pattern: str) -> None:
        """パターンに一致するキャッシュファイルを削除"""
        regex = re.compile(pattern)
        for file_path in self._cache_dir.glob('*.json'):
            if regex.search(file_path.stem):
                try:
                    file_path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning("Failed to remove cache file %s: %s", file_path, exc)

    def clear_all(self) -> None:
        """キャッシュディレクトリをクリア"""
        for file_path in self._cache_dir.glob('*.json'):
            try:
                file_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("Failed to remove cache file %s: %s", file_path, exc)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _load_entry(self, path: Path) -> Optional[CacheEntry]:
        if not self.enabled:
            return None
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except Exception as exc:
            logger.warning('Failed to load cache file %s: %s', path, exc)
            return None
        timestamp = payload.get('timestamp')
        data = payload.get('data')
        if timestamp is None:
            return None
        try:
            cached_at = datetime.fromisoformat(timestamp)
        except ValueError:
            return None
        expired = self._is_expired(cached_at)
        if self._refresh_on_start and not self._refresh_consumed:
            expired = True
        return CacheEntry(data=data, expired=expired)

    def _save_entry(self, path: Path, data: Any, ttl_override: Optional[int] = None) -> None:
        if not self.enabled:
            return
        payload = {
            'timestamp': datetime.utcnow().isoformat(),
            'data': data,
            'ttl_seconds': ttl_override if ttl_override is not None else self._ttl_hours * 3600 if self._ttl_hours else None,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        if self._refresh_on_start:
            self._refresh_consumed = True

    def _is_expired(self, cached_at: datetime) -> bool:
        if self._ttl_hours <= 0:
            return False
        expiry = cached_at + timedelta(hours=self._ttl_hours)
        return datetime.utcnow() > expiry

    def _key_to_path(self, key: str) -> Path:
        safe_key = re.sub(r'[^0-9A-Za-z_.-]', '_', key)
        return self._cache_dir / f'{safe_key}.json'

    def _ensure_cache_readme(self) -> None:
        readme_path = self._cache_dir / CACHE_README_FILENAME
        if readme_path.exists():
            return
        try:
            readme_path.write_text(CACHE_README_TEXT, encoding='utf-8')
        except Exception as exc:  # pragma: no cover - readme write failure should not break runtime
            logger.warning("Failed to write cache README %s: %s", readme_path, exc)


def _is_enabled_env(name: str) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


CACHE_MANAGER = CacheManager()
