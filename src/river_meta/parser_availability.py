from __future__ import annotations

import re

from bs4 import BeautifulSoup


DECADE_RE = re.compile(r"(\d{3})\*")


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u3000", " ")).strip()


def _cell_has_data(cell) -> bool:  # BeautifulSoup Tag type kept generic intentionally.
    for image in cell.find_all("img"):
        src = (image.get("src") or "").lower()
        alt = _normalize_text(image.get("alt") or "").lower()
        if "ari.gif" in src:
            return True
        if alt in {"有", "あり", "data"}:
            return True

    text = _normalize_text(cell.get_text(" ", strip=True))
    return text in {"有", "あり", "○", "1"}


def parse_availability_page(html: str) -> tuple[list[int], bool]:
    soup = BeautifulSoup(html, "html.parser")
    years: list[int] = []
    found_decade_row = False

    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if not cells:
            continue

        texts = [_normalize_text(cell.get_text(" ", strip=True)) for cell in cells]
        decade_index = -1
        decade_start = -1

        for idx, text in enumerate(texts):
            match = DECADE_RE.search(text)
            if not match:
                continue
            decade_index = idx
            decade_start = int(match.group(1)) * 10
            found_decade_row = True
            break

        if decade_index < 0:
            continue

        for offset in range(10):
            target_index = decade_index + 1 + offset
            if target_index >= len(cells):
                break
            if _cell_has_data(cells[target_index]):
                years.append(decade_start + offset)

    return sorted(set(years)), found_decade_row
