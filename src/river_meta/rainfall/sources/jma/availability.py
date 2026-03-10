from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup


JmaAvailabilityStatus = Literal["success_with_years", "success_empty", "indeterminate"]


@dataclass(slots=True)
class JmaAvailabilityResult:
    status: JmaAvailabilityStatus
    years: set[int] = field(default_factory=set)
    reason: str = ""


def build_jma_index_url(prec_no: str, block_no: str) -> str:
    prec = str(prec_no).strip().zfill(2)
    block = str(block_no).strip()
    return (
        "https://www.data.jma.go.jp/stats/etrn/index.php"
        f"?prec_no={prec}&block_no={block}&year=&month=&day=&view="
    )


def parse_available_years_from_index_html(
    html: str,
    *,
    prec_no: str,
    block_no: str,
) -> tuple[set[int], bool]:
    soup = BeautifulSoup(html, "html.parser")
    years: set[int] = set()
    context_matched = False

    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href", "")).strip()
        if not href:
            continue
        parsed = urlparse(href)
        if not parsed.path.lower().endswith("index.php"):
            continue

        query = parse_qs(parsed.query, keep_blank_values=True)
        link_prec = _first_query_value(query, "prec_no")
        link_block = _first_query_value(query, "block_no")

        if not _codes_match(link_prec, str(prec_no)) or not _codes_match(link_block, str(block_no)):
            continue

        context_matched = True
        year_value = _first_query_value(query, "year")
        if year_value.isdigit() and len(year_value) == 4:
            years.add(int(year_value))

    return years, context_matched


def fetch_available_years_hourly(
    *,
    prec_no: str,
    block_no: str,
    timeout_sec: float = 10.0,
    user_agent: str = "river-meta/0.1",
) -> JmaAvailabilityResult:
    url = build_jma_index_url(prec_no, block_no)
    headers = {"User-Agent": user_agent}

    try:
        response = requests.get(url, headers=headers, timeout=timeout_sec)
    except requests.RequestException as exc:
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason=f"request_failed:{type(exc).__name__}",
        )

    if response.status_code >= 400:
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason=f"http_status:{response.status_code}",
        )

    years, context_matched = parse_available_years_from_index_html(
        response.text,
        prec_no=prec_no,
        block_no=block_no,
    )

    if not context_matched:
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )
    if years:
        return JmaAvailabilityResult(
            status="success_with_years",
            years=years,
            reason="matched_index_links",
        )
    return JmaAvailabilityResult(
        status="success_empty",
        years=set(),
        reason="matched_index_links_but_no_years",
    )


def _first_query_value(query: dict[str, list[str]], key: str) -> str:
    values = query.get(key, [])
    if not values:
        return ""
    return str(values[0]).strip()


def _codes_match(left: str, right: str) -> bool:
    left_code = str(left).strip()
    right_code = str(right).strip()
    if not left_code or not right_code:
        return False
    if left_code.isdigit() and right_code.isdigit():
        return int(left_code) == int(right_code)
    return left_code == right_code
