from src.water_info.infra import http_client


def test_calc_delay_basics():
    assert http_client._calc_delay(0) == 0.0
    assert http_client._calc_delay(1) == http_client.REQUEST_MIN_DELAY


def test_calc_delay_caps_at_max():
    delay = http_client._calc_delay(999)
    assert delay == http_client.REQUEST_MAX_DELAY
