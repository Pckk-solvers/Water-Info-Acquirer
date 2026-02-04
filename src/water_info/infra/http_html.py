"""HTTP + HTML helpers for water_info."""

from __future__ import annotations

from bs4 import BeautifulSoup


def fetch_html(throttled_get, headers: dict, url: str, encoding: str = "euc_jp") -> str:
    res = throttled_get(url, headers=headers)
    res.encoding = encoding
    return res.text


def parse_html(html: str):
    return BeautifulSoup(html, "html.parser")
