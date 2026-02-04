"""Station name scraping utilities."""

from __future__ import annotations

import re
from typing import Protocol


class _SoupLike(Protocol):
    def find_all(self, name: str, attrs=None):
        ...


def extract_station_name(soup: _SoupLike) -> str:
    """観測所名をHTMLから抽出し、読み仮名を除去して返す。"""
    info_table = soup.find_all("table", {"border": "1", "cellpadding": "2", "cellspacing": "1"})[0]
    data_tr = info_table.find_all("tr")[1]
    cells = data_tr.find_all("td")
    raw_name = cells[1].get_text(strip=True)
    return re.sub(r"（.*?）", "", raw_name).strip()
