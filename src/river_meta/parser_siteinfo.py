from __future__ import annotations

import re

from bs4 import BeautifulSoup

from .models import SiteMeta


LABEL_SUFFIXES = (
    "名",
    "所在地",
    "緯度",
    "経度",
    "緯度経度",
    "管理者",
    "管理",
    "水系",
    "河川",
    "住所",
)


def _normalize_text(value: str) -> str:
    normalized = value.replace("\u3000", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _normalize_label(value: str) -> str:
    label = _normalize_text(value)
    label = label.rstrip(":：")
    return label.strip()


def _add_meta(meta: SiteMeta, label: str, value: str) -> None:
    if label in meta:
        current = meta[label]
        if isinstance(current, list):
            if value not in current:
                current.append(value)
            return
        if current == value:
            return
        meta[label] = [current, value]
        return
    meta[label] = value


def _is_label_candidate(label: str) -> bool:
    if not label or len(label) > 40:
        return False
    if label.startswith("http"):
        return False
    if any(token in label for token in ("<", ">", "{", "}")):
        return False
    if any(label.endswith(suffix) for suffix in LABEL_SUFFIXES):
        return True
    if len(label) <= 16 and not re.search(r"\d{4,}", label):
        return True
    return False


def parse_siteinfo(html: str) -> SiteMeta:
    soup = BeautifulSoup(html, "html.parser")
    meta: SiteMeta = {}

    for row in soup.select("table tr"):
        cells = row.find_all(["th", "td"])
        texts = [_normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
        texts = [text for text in texts if text]
        if len(texts) < 2:
            continue

        for idx in range(0, len(texts) - 1, 2):
            label = _normalize_label(texts[idx])
            value = texts[idx + 1]
            if not _is_label_candidate(label) or not value:
                continue
            _add_meta(meta, label, value)

    if meta:
        return meta

    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        label = _normalize_label(dt.get_text(" ", strip=True))
        value = _normalize_text(dd.get_text(" ", strip=True))
        if _is_label_candidate(label) and value:
            _add_meta(meta, label, value)

    return meta
