"""HTML scraping helpers for water_info."""

from __future__ import annotations

from typing import Iterable


def extract_font_values(soup) -> list[str]:
    return [f.get_text() for f in soup.select("td > font")]


def coerce_numeric_series(pd, values: Iterable[str]):
    return pd.to_numeric(pd.Series(values), errors="coerce").tolist()
