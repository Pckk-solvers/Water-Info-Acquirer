import pytest

from src.water_info import entry


def _capture_first_url(monkeypatch, module):
    captured = {"url": None}

    def _stub(url, headers=None, timeout=30):
        captured["url"] = url
        raise RuntimeError("stop")

    monkeypatch.setattr(module, "throttled_get", _stub)
    return captured


@pytest.mark.parametrize(
    "mode_type, expected_kind, expected_segment",
    [
        ("S", "KIND=2", "DspWaterData.exe"),
        ("R", "KIND=6", "DspWaterData.exe"),
        ("U", "KIND=2", "DspRainData.exe"),
    ],
)
def test_process_data_for_code_builds_expected_url(monkeypatch, mode_type, expected_kind, expected_segment):
    captured = _capture_first_url(monkeypatch, entry)
    with pytest.raises(RuntimeError):
        entry.process_data_for_code(
            code="123",
            Y1="2024",
            Y2="2024",
            M1="2月",
            M2="2月",
            mode_type=mode_type,
            single_sheet=False,
        )
    url = captured["url"]
    assert url is not None
    assert expected_segment in url
    assert expected_kind in url
    assert "ID=123" in url
    assert "BGNDATE=20240201" in url
    assert "ENDDATE=20241231" in url


@pytest.mark.parametrize(
    "mode_type, expected_kind, expected_segment",
    [
        ("S", "KIND=3", "DspWaterData.exe"),
        ("R", "KIND=7", "DspWaterData.exe"),
        ("U", "KIND=3", "DspRainData.exe"),
    ],
)
def test_process_period_date_display_for_code_builds_expected_url(monkeypatch, mode_type, expected_kind, expected_segment):
    captured = _capture_first_url(monkeypatch, entry)
    with pytest.raises(RuntimeError):
        entry.process_period_date_display_for_code(
            code="999",
            Y1="2023",
            Y2="2024",
            M1="1月",
            M2="12月",
            mode_type=mode_type,
            single_sheet=False,
        )
    url = captured["url"]
    assert url is not None
    assert expected_segment in url
    assert expected_kind in url
    assert "ID=999" in url
    assert "BGNDATE=20230101" in url
    assert "ENDDATE=20231231" in url
