from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.water_info.domain.dam import DAM_METRICS, DamRequest, iter_dam_windows
from src.water_info.infra.dam_parser import parse_dam_download
from src.water_info.infra.dam_writer import write_dam_outputs
from src.water_info.service.dam import fetch_dam_data

CODE = "608061288803010"


def payload(day="2024/12/31", hour="24:00", values=None, code=CODE):
    header = ["#年月日", "時刻"] + [x for m in DAM_METRICS for x in (m.label, m.label + "属性")]
    values = values or ["0", " ", "0", "#", "0", "$", "0", "-", "1", "?"]
    text = "\n".join([f"観測所記号,{code}", "観測所名,長安口ダム", "水系名,那賀川",
                      "河川名,那賀川", ",".join(header), ",".join([day, hour] + values)])
    return text.encode("cp932")


def getter(content):
    def get(url, **kwargs):
        if "DspDamData" in url:
            return SimpleNamespace(content=b'<a href="/dat/dload/download/sample.dat">download</a>')
        return SimpleNamespace(content=content)
    return get


def test_windows_month_boundary_leap_and_limit():
    windows = list(iter_dam_windows(date(2024, 1, 31), date(2024, 3, 1)))
    assert windows == [(date(2024, 1, 31), date(2024, 1, 31)),
                       (date(2024, 2, 1), date(2024, 2, 29)),
                       (date(2024, 3, 1), date(2024, 3, 1))]
    assert all((b - a).days + 1 <= 31 for a, b in windows)
    with pytest.raises(ValueError):
        DamRequest(date(2024, 2, 1), date(2024, 1, 1), Path("out"))


def test_parse_year_end_preserves_zero_raw_and_each_quality():
    df, meta = parse_dam_download(payload(), CODE, date(2024, 12, 31), date(2024, 12, 31))
    row = df.iloc[0]
    assert meta["観測所名"] == "長安口ダム"
    assert row["datetime"] == pd.Timestamp("2025-01-01")
    assert row["source_date"] == "2024-12-31"
    assert row["basin_rainfall"] == 0
    for metric, quality in [("storage", "closed"), ("inflow", "missing"),
                             ("outflow", "unreceived"), ("storage_rate", "unknown")]:
        assert row[metric] is None
        assert row[metric + "_quality"] == quality
    assert row["storage_raw"] == "0" and row["storage_attribute"] == "#"


@pytest.mark.parametrize("raw", ["", "NaN", "inf", "abc"])
def test_nonnumeric_without_attribute_is_missing(raw):
    df, _ = parse_dam_download(payload(values=[raw, " "] * 5), CODE,
                               date(2024, 12, 31), date(2024, 12, 31))
    assert df.iloc[0]["storage_quality"] == "missing"


@pytest.mark.parametrize("content", [payload(code="123"), payload(hour="00:00"),
    payload(hour="01:10"), payload(day="2024/12/30"),
    payload().replace("貯水量属性".encode("cp932"), b"other"),
    payload() + b"\n" + payload().splitlines()[-1]])
def test_reject_wrong_station_time_range_header_and_duplicate(content):
    with pytest.raises(ValueError):
        parse_dam_download(content, CODE, date(2024, 12, 31), date(2024, 12, 31))


def test_fetch_reindex_cache_and_export_roundtrip(tmp_path):
    request = DamRequest(date(2024, 12, 31), date(2024, 12, 31), tmp_path, True)
    data = fetch_dam_data(CODE, request, get=getter(payload()))
    assert data.status == "complete" and len(data.frame) == 24
    assert data.windows[0]["missing_rows"] == 23
    assert data.frame["storage_quality"].tolist() == ["row_missing"] * 23 + ["closed"]

    def no_network(*args, **kwargs):
        raise AssertionError("キャッシュ済みの窓は通信しない")

    resumed = fetch_dam_data(CODE, request, get=no_network)
    assert resumed.windows[0]["cached"] is True
    outputs = write_dam_outputs(resumed)
    excel = pd.read_excel(outputs["excel"][0], sheet_name=None)
    assert len(outputs["excel"]) == 1
    assert set(excel) == {"2024", "年別品質", "品質一覧", "出典", "取得区間"}
    assert excel["2024"]["日時"].iloc[-1] == pd.Timestamp("2025-01-01")
    assert len(list(tmp_path.glob("*.xlsx"))) == 1
    assert len(list(tmp_path.glob("*.csv"))) == 1
    pq = pd.read_parquet(outputs["parquet"])
    assert pq.iloc[-1]["storage_raw"] == "0"
    assert pd.isna(pq.iloc[-1]["storage"])
    summary = json.loads(Path(outputs["summary"]).read_text(encoding="utf-8"))
    assert summary["quality"]["storage"]["closed"] == 1
    assert summary["quality"]["storage"]["row_missing"] == 23
    assert summary["published_rows"] == 1
    overview = pd.read_excel(outputs["overview_excel"], sheet_name="年別品質")
    assert len(overview) == 5
    assert overview["公開時間行数"].eq(1).all()


def test_multiple_years_are_sheets_in_one_workbook(tmp_path):
    request = DamRequest(date(2023, 12, 31), date(2024, 1, 1), tmp_path)

    def get(url, **kwargs):
        if "DspDamData" in url:
            day = "2023/12/31" if "BGNDATE=20231231" in url else "2024/1/1"
            return SimpleNamespace(content=f'<a href="/dat/dload/download/{day.replace("/", "")}.dat">data</a>'.encode())
        day = "2023/12/31" if "20231231.dat" in url else "2024/1/1"
        return SimpleNamespace(content=payload(day=day))

    data = fetch_dam_data(CODE, request, get=get)
    outputs = write_dam_outputs(data)
    assert len(list(tmp_path.glob("*.xlsx"))) == 1
    sheets = pd.read_excel(outputs["excel"][0], sheet_name=None)
    assert list(sheets) == ["2023", "2024", "年別品質", "品質一覧", "出典", "取得区間"]
    assert len(sheets["2023"]) == len(sheets["2024"]) == 24
    assert sheets["2023"]["日時"].iloc[-1] == pd.Timestamp("2024-01-01")
    assert sheets["2024"]["日時"].iloc[0] == pd.Timestamp("2024-01-01 01:00")
    assert len(sheets["取得区間"]) == 2


def test_network_failure_and_cancel_are_not_success(tmp_path):
    request = DamRequest(date(2024, 12, 31), date(2024, 12, 31), tmp_path)

    def failure(*args, **kwargs):
        raise RuntimeError("network failure")

    failed = fetch_dam_data(CODE, request, get=failure)
    assert failed.status == "partial"
    assert failed.windows[0]["error"] == "network failure"
    assert failed.frame["inflow_quality"].eq("fetch_failed").all()
    cancelled = fetch_dam_data(CODE, request, get=failure, should_stop=lambda: True)
    assert cancelled.status == "cancelled"
    assert cancelled.windows[0]["status"] == "cancelled"


def test_valid_empty_download_is_publication_gap_not_network_failure(tmp_path):
    # 実データで確認した2009年初めのような、ヘッダーのみの配布データ。
    empty = b"\n".join(payload().splitlines()[:-1])
    request = DamRequest(date(2024, 12, 31), date(2024, 12, 31), tmp_path)
    data = fetch_dam_data(CODE, request, get=getter(empty))
    assert data.status == "complete"
    assert data.windows[0]["rows"] == 0
    assert data.windows[0]["missing_rows"] == 24
    assert data.frame["storage_quality"].eq("row_missing").all()
    outputs = write_dam_outputs(data)
    summary = json.loads(Path(outputs["summary"]).read_text(encoding="utf-8"))
    assert summary["published_rows"] == 0
    assert summary["quality"]["storage"]["fetch_failed"] == 0


def test_corrupt_cache_refetched_and_foreign_link_rejected(tmp_path):
    request = DamRequest(date(2024, 12, 31), date(2024, 12, 31), tmp_path)
    data = fetch_dam_data(CODE, request, get=getter(payload()))
    Path(data.windows[0]["source_file"]).write_bytes(b"corrupt")
    resumed = fetch_dam_data(CODE, request, get=getter(payload()))
    assert resumed.status == "complete" and not resumed.windows[0]["cached"]

    def foreign(*args, **kwargs):
        return SimpleNamespace(content=b'<a href="https://example.com/dat/dload/a.dat">bad</a>')

    other = DamRequest(date(2024, 12, 30), date(2024, 12, 30), tmp_path)
    rejected = fetch_dam_data(CODE, other, get=foreign)
    assert rejected.status == "partial"
