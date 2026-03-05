"""HTTP + HTML helpers for water_info."""

from __future__ import annotations

from bs4 import BeautifulSoup


def fetch_html(
    throttled_get,
    headers: dict,
    url: str,
    encoding: str = "euc_jp",
    should_stop=None,
) -> str:
    if should_stop is None:
        res = throttled_get(url, headers=headers)
    else:
        res = throttled_get(url, headers=headers, should_stop=should_stop)
    res.encoding = encoding
    return res.text


def parse_html(html: str):
    return BeautifulSoup(html, "html.parser")
