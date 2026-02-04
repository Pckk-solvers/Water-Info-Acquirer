from src.water_info.infra import fetching


def test_fetch_hourly_values_drop_last_each(monkeypatch):
    urls = ["u1", "u2"]
    payloads = {
        "u1": ["1", "2", "3"],
        "u2": ["4", "5", "6"],
    }

    def _fake_fetch(_get, _headers, _bs, url):
        return payloads[url]

    monkeypatch.setattr(fetching, "fetch_font_values", _fake_fetch)

    values = fetching.fetch_hourly_values(
        throttled_get=None,
        headers={},
        BeautifulSoup=None,
        urls=urls,
        drop_last_each=True,
    )

    assert values == [1.0, 2.0, 4.0, 5.0]
