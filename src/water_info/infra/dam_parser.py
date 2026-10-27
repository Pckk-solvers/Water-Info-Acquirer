"""ダム配布CSVの原値と項目別品質を保持して解析する。"""

import csv
from datetime import date
from io import StringIO
import math
import re

import pandas as pd

from ..domain.dam import DAM_METRICS


def dam_columns():
    return ["station_code", "source_date", "source_time", "datetime"] + [
        col for m in DAM_METRICS
        for col in (m.key, f"{m.key}_raw", f"{m.key}_attribute", f"{m.key}_quality")
    ]


def parse_dam_download(content: bytes, code: str, start: date, end: date):
    text = content.decode("cp932")
    rows = list(csv.reader(StringIO(text)))
    metadata = {}
    header = None
    expected = ["#年月日", "時刻"] + [
        name for m in DAM_METRICS for name in (m.label, m.label + "属性")
    ]
    records = []
    seen = set()
    for row in rows:
        if not row:
            continue
        if row[0] in {"水系名", "河川名", "観測所名", "観測所記号"} and len(row) == 2:
            metadata[row[0]] = row[1].strip()
        if row[0] == "#年月日":
            if [v.strip() for v in row] != expected:
                raise ValueError("ダム配布データの項目構成が未対応です")
            header = row
            continue
        if not re.match(r"^\d{4}/", row[0]):
            continue
        if header is None or len(row) != len(expected):
            raise ValueError("ダム配布データのヘッダーまたは列数が不正です")
        day = date(*map(int, row[0].split("/")))
        if not start <= day <= end:
            raise ValueError(f"要求期間外の日付です: {day}")
        if not re.fullmatch(r"(?:0[1-9]|1\d|2[0-4]):00", row[1]):
            raise ValueError(f"時間値ではない時刻です: {row[1]}")
        timestamp = pd.Timestamp(day) + pd.Timedelta(hours=int(row[1][:2]))
        if timestamp in seen:
            raise ValueError(f"重複時刻です: {timestamp}")
        seen.add(timestamp)
        record = {"station_code": code, "source_date": day.isoformat(),
                  "source_time": row[1], "datetime": timestamp}
        for i, metric in enumerate(DAM_METRICS):
            raw, attr = row[2 + i * 2].strip(), row[3 + i * 2].strip()
            quality = {"$": "missing", "-": "unreceived", "#": "closed"}.get(attr)
            if attr and quality is None:
                quality = "unknown"
            try:
                number = float(raw)
                if not math.isfinite(number):
                    number = None
            except ValueError:
                number = None
            if quality is None:
                quality = "normal" if number is not None else "missing"
            record.update({metric.key: number if quality == "normal" else None,
                           f"{metric.key}_raw": raw, f"{metric.key}_attribute": attr,
                           f"{metric.key}_quality": quality})
        records.append(record)
    if metadata.get("観測所記号") != code:
        raise ValueError("ダム配布データの観測所記号が一致しません")
    if header is None:
        raise ValueError("ダム配布データのヘッダーがありません")
    return pd.DataFrame(records, columns=pd.Index(dam_columns())).sort_values("datetime"), metadata
