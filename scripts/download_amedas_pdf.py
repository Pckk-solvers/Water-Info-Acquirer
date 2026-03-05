# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests",
# ]
# ///

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import requests


DEFAULT_URL = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.pdf"
DEFAULT_OUTPUT = Path("data/source/amedas/ame_master.pdf")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AMeDASマスタPDFをダウンロードして保存する")
    parser.add_argument("--url", default=DEFAULT_URL, help="取得元URL")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="保存先PDFパス")
    parser.add_argument("--metadata", default="", help="メタデータJSONパス")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTPタイムアウト秒")
    return parser.parse_args(argv)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _save_text_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _save_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(data)
    tmp.replace(path)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    output_path = Path(args.output)
    metadata_path = Path(args.metadata) if args.metadata else output_path.with_name("metadata.json")

    print(f"ダウンロード開始: {args.url}")
    response = requests.get(args.url, timeout=args.timeout)
    response.raise_for_status()

    pdf_bytes = response.content
    new_sha256 = _sha256_bytes(pdf_bytes)
    old_sha256 = _sha256_file(output_path) if output_path.exists() else ""
    changed = old_sha256 != new_sha256

    if changed:
        _save_bytes(output_path, pdf_bytes)
        print(f"PDF更新: {output_path}")
    else:
        print(f"PDF変更なし: {output_path}")

    metadata = {
        "url": args.url,
        "final_url": response.url,
        "downloaded_at": datetime.now(timezone.utc).isoformat(),
        "content_type": response.headers.get("Content-Type", ""),
        "content_length": len(pdf_bytes),
        "sha256": new_sha256,
        "changed": changed,
        "output_path": str(output_path),
    }
    _save_text_json(metadata_path, metadata)
    print(f"メタデータ保存: {metadata_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
