import types
from pathlib import Path

import pytest
import requests

import src.main_datetime as main_dt
import src.datemode as date_mod


META_HTML = """
<html>
  <body>
    <table border="1" cellpadding="2" cellspacing="1">
      <tr><td>idx</td><td>name</td></tr>
      <tr><td>001</td><td>テスト観測所</td></tr>
    </table>
  </body>
</html>
"""

DATA_HTML = """
<html>
  <body>
    <td><font>1.0</font></td>
    <td><font>2.0</font></td>
    <td><font>3.0</font></td>
  </body>
</html>
"""

BAD_DATA_HTML = """
<html>
  <body>
    <td><font>---</font></td>
  </body>
</html>
"""


class DummyResponse:
    """requests.Response 代替として minimum API を提供するテスト用レスポンス"""

    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _patch_throttled_get(monkeypatch, target_module, payloads):
    """与えた payloads を順番に返すよう throttled_get を差し替える"""
    sequence = iter(payloads)

    def _fake_throttled_get(url, headers, timeout=30):
        try:
            return next(sequence)
        except StopIteration:
            raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr(target_module, "throttled_get", _fake_throttled_get)


# 1時間データ処理が正常にExcel出力されることを確認するテスト
def test_process_data_for_code_outputs_excel(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_throttled_get(
        monkeypatch,
        main_dt,
        [DummyResponse(META_HTML), DummyResponse(DATA_HTML)],
    )

    result = main_dt.process_data_for_code(
        code="123456789012345",
        Y1="2024",
        Y2="2024",
        M1="1月",
        M2="1月",
        mode_type="S",
        single_sheet=False,
    )

    output_path = tmp_path / result
    assert output_path.exists()
    assert result.endswith("_WH.xlsx")


# 日別処理が年単位のURL分割を経てExcelを生成することを確認するテスト
def test_process_period_date_display_for_code_outputs_excel(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_throttled_get(
        monkeypatch,
        date_mod,
        [DummyResponse(META_HTML), DummyResponse(DATA_HTML)],
    )

    result = date_mod.process_period_date_display_for_code(
        code="123456789012345",
        Y1="2023",
        Y2="2023",
        M1="1月",
        M2="3月",
        mode_type="S",
        single_sheet=False,
    )

    output_path = tmp_path / result
    assert output_path.exists()
    assert result.endswith("_WD.xlsx")


# 数値が1件も得られないケースでは ValueError を返していることを確認
def test_process_data_for_code_invalid_values_raise_warning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _patch_throttled_get(
        monkeypatch,
        main_dt,
        [DummyResponse(META_HTML), DummyResponse(BAD_DATA_HTML)],
    )

    with pytest.raises(ValueError):
        main_dt.process_data_for_code(
            code="123456789012345",
            Y1="2024",
            Y2="2024",
            M1="1月",
            M2="1月",
            mode_type="S",
            single_sheet=False,
        )


# throttled_get が 429 応答を待機＋再試行の後に成功させることを確認
def test_throttled_get_retries_on_retryable_status(monkeypatch):
    date_mod._REQUEST_COUNTER = 0
    date_mod._ensure_http_client()

    sleeps = []

    def fake_sleep(value):
        sleeps.append(value)

    monkeypatch.setattr(date_mod.time, "sleep", fake_sleep)

    responses = iter(
        [
            DummyResponse("retry", status_code=429),
            DummyResponse("ok", status_code=200),
        ]
    )

    def fake_get(url, headers, timeout):
        return next(responses)

    monkeypatch.setattr(date_mod.requests, "get", fake_get)

    resp = date_mod.throttled_get("http://example.com", headers={})
    assert resp.text == "ok"
    assert sleeps == [
        date_mod.REQUEST_MIN_DELAY,  # バックオフ
        date_mod.REQUEST_MIN_DELAY,  # 2回目開始前のインターバル
    ]
