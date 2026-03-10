from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.sources.jma.availability import (  # noqa: E402
    JmaAvailabilityResult,
    build_jma_index_url,
    fetch_available_years_hourly,
    parse_available_years_from_index_html,
)


def test_build_jma_index_url():
    url = build_jma_index_url("1", "47401")
    assert (
        url
        == "https://www.data.jma.go.jp/stats/etrn/index.php"
        "?prec_no=01&block_no=47401&year=&month=&day=&view="
    )


def test_parse_available_years_from_index_html_uses_only_same_station_links():
    html = """
    <html><body>
      <a href="index.php?prec_no=11&block_no=47401&year=2024&month=01&day=01&view=">2024</a>
      <a href="./index.php?prec_no=11&block_no=47401&year=2023&month=01&day=01&view=">2023</a>
      <a href="index.php?prec_no=11&block_no=47401&year=2023&month=02&day=01&view=">2023-dup</a>
      <a href="index.php?prec_no=11&block_no=99999&year=2022&month=01&day=01&view=">other-block</a>
      <a href="index.php?prec_no=12&block_no=47401&year=2021&month=01&day=01&view=">other-prec</a>
      <a href="view/hourly_s1.php?prec_no=11&block_no=47401&year=2020&month=01&day=01&view=">not-index</a>
    </body></html>
    """
    years, context_matched = parse_available_years_from_index_html(
        html,
        prec_no="11",
        block_no="47401",
    )
    assert years == {2023, 2024}
    assert context_matched is True


def test_fetch_available_years_hourly_returns_indeterminate_on_context_mismatch(monkeypatch):
    class _FakeResponse:
        status_code = 200
        text = (
            "<a href='index.php?prec_no=99&block_no=88888&year=2025&month=1&day=1&view='>2025</a>"
        )

    def _fake_get(url, *, headers, timeout):  # noqa: ARG001
        return _FakeResponse()

    monkeypatch.setattr("river_meta.rainfall.sources.jma.availability.requests.get", _fake_get)

    result = fetch_available_years_hourly(prec_no="11", block_no="47401")
    assert isinstance(result, JmaAvailabilityResult)
    assert result.status == "indeterminate"
    assert result.years == set()
    assert result.reason == "context_mismatch"
