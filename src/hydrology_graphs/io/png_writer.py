from __future__ import annotations

from pathlib import Path


def write_png(path: str | Path, payload: bytes) -> Path:
    """PNG バイト列を指定パスへ保存する。"""

    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(payload)
    return file_path
