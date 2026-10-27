"""ダム諸量を1冊の年別シートと全期間データへ整理する。"""

from datetime import datetime
import json
from pathlib import Path

import pandas as pd

from ..domain.dam import DAM_METRICS, QUALITY_LABELS


def quality_summary(frame):
    return {m.key: {q: int(frame[f"{m.key}_quality"].eq(q).sum())
                    for q in QUALITY_LABELS} for m in DAM_METRICS}


def quality_table(frame):
    counts = quality_summary(frame)
    return pd.DataFrame([
        {"項目": m.label, "単位": m.unit, "期待件数": len(frame),
         **{label: counts[m.key][q] for q, label in QUALITY_LABELS.items()}}
        for m in DAM_METRICS
    ])


def excel_frame(frame):
    names = {"station_code": "観測所記号", "source_date": "元日付",
             "source_time": "元時刻", "datetime": "日時"}
    columns = ["station_code", "source_date", "source_time", "datetime"]
    for m in DAM_METRICS:
        names[m.key] = f"{m.label}[{m.unit}]"
        columns.append(m.key)
    for m in DAM_METRICS:
        for suffix, label in (("_quality", "品質"), ("_raw", "原値"), ("_attribute", "属性")):
            names[m.key + suffix] = m.label + label
            columns.append(m.key + suffix)
    result = frame[columns].copy()
    for m in DAM_METRICS:
        result[m.key + "_quality"] = result[m.key + "_quality"].map(QUALITY_LABELS)
    return result.rename(columns=names)


def write_dam_outputs(data, *, progress=None):
    root = Path(data.request.output_dir)
    root.mkdir(parents=True, exist_ok=True)
    frame = data.frame
    expected_rows = ((data.request.end - data.request.start).days + 1) * 24
    if len(frame) != expected_rows or frame["datetime"].duplicated().any():
        raise ValueError("出力行数または日時の一意性が要求期間と一致しません")
    stem = f"{data.code}_dam_{data.request.start:%Y%m%d}_{data.request.end:%Y%m%d}"
    csv_path = root / f"{stem}.csv"
    frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
    output = {"csv": str(csv_path), "excel": []}
    if data.request.export_parquet:
        p = root / f"{stem}.parquet"
        frame.to_parquet(p, index=False)
        output["parquet"] = str(p)
    source_rows = [
        ("出典", "国土交通省 水文水質データベース"),
        *data.metadata.items(),
        ("要求元日付開始", str(data.request.start)), ("要求元日付終了", str(data.request.end)),
        ("出力日時", datetime.now().astimezone().isoformat()),
        ("データ種別", "任意期間ダム諸量・時間値 KIND=1"),
        ("時刻", "日本標準時。元日付の24時は翌日0時。年別は元日付の年で分類。"),
        ("品質", "閉局・欠測・未受信・不明属性は整理値を空欄とし原値と属性を保持。"),
        ("集計", "雨量積算・流量平均等の独自集計や補間は実施しない。"),
        ("取得状態", data.status),
    ]
    years = []
    for year, group in frame.groupby(frame["source_date"].str[:4], sort=True):
        published = ~group[f"{DAM_METRICS[0].key}_quality"].isin(["row_missing", "fetch_failed"])
        years.append({"year": str(year), "expected_rows": len(group),
                      "published_rows": int(published.sum()),
                      "first_published_at": str(group.loc[published, "datetime"].min()) if published.any() else None,
                      "last_published_at": str(group.loc[published, "datetime"].max()) if published.any() else None,
                      "quality": quality_summary(group)})
    annual_quality = []
    for year in years:
        for m in DAM_METRICS:
            annual_quality.append({"年": year["year"], "項目": m.label,
                "期待時間行数": year["expected_rows"], "公開時間行数": year["published_rows"],
                **{label: year["quality"][m.key][q] for q, label in QUALITY_LABELS.items()}})
    workbook = root / f"{stem}.xlsx"
    with pd.ExcelWriter(workbook, engine="xlsxwriter",
                        engine_kwargs={"options": {"strings_to_formulas": False,
                                                    "strings_to_urls": False}}) as writer:
        for year, group in frame.groupby(frame["source_date"].str[:4], sort=True):
            if progress:
                progress(f"{year}年シートを作成中")
            human = excel_frame(group)
            human.to_excel(writer, sheet_name=str(year), index=False)
            ws = writer.sheets[str(year)]
            ws.set_column(3, 3, 21)
            ws.autofilter(0, 0, len(human), len(human.columns) - 1)
        pd.DataFrame(annual_quality).to_excel(writer, sheet_name="年別品質", index=False)
        quality_table(frame).to_excel(writer, sheet_name="品質一覧", index=False)
        pd.DataFrame(source_rows, columns=pd.Index(["項目", "内容"])).to_excel(
            writer, sheet_name="出典", index=False)
        pd.DataFrame(data.windows).to_excel(writer, sheet_name="取得区間", index=False)
        for name, ws in writer.sheets.items():
            ws.freeze_panes(1, 0)
            ws.set_column(0, 2, 18)
            ws.set_column(4, 30, 18)
            if name not in {y["year"] for y in years}:
                ws.set_column(3, 3, 18)
            if name == "出典":
                ws.set_column(1, 1, 85)
    output["excel"].append(str(workbook))
    output["overview_excel"] = str(workbook)
    summary = {
        "station_code": data.code, "metadata": data.metadata, "status": data.status,
        "start": str(data.request.start), "end": str(data.request.end),
        "expected_rows": expected_rows, "rows": len(frame),
        "published_rows": sum(y["published_rows"] for y in years),
        "metrics": [{"key": m.key, "label": m.label, "unit": m.unit} for m in DAM_METRICS],
        "quality": quality_summary(frame), "years": years, "windows": data.windows,
        "outputs": output,
    }
    summary_path = root / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    output["summary"] = str(summary_path)
    report = [f"# ダム諸量取得結果: {data.metadata.get('観測所名', data.code)}", "",
              f"観測所記号: {data.code} / 元日付: {data.request.start}〜{data.request.end}", "",
              f"取得状態: {data.status} / 期待時間行数: {expected_rows:,}", "",
              "Excel1冊に年別時間値・年別品質・品質一覧・出典・取得区間を収録。CSVは全期間1本。", "",
              "数値が空欄の場合は品質列を確認してください。補間や0埋めは行っていません。",
              "元日付の24時を翌日0時に正規化し、年別シートは元日付で分類しています。",
              "CSVとParquetのstorageの単位は10^3 m3です。", "",
              "|項目|有効|欠測|未受信|閉局|不明属性|行欠落|取得失敗|",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    counts = summary["quality"]
    for m in DAM_METRICS:
        report.append("|" + m.label + "|" + "|".join(str(counts[m.key][q]) for q in QUALITY_LABELS) + "|")
    report += ["", "## 年別の公開状況", "",
               "|年|期待時間行数|公開時間行数|最初の公開日時|最後の公開日時|",
               "|---|---:|---:|---|---|"]
    for year in years:
        report.append(f"|{year['year']}|{year['expected_rows']}|{year['published_rows']}|"
                      f"{year['first_published_at'] or 'なし'}|{year['last_published_at'] or 'なし'}|")
    report += ["", "公開時間行数は元配布データに存在した行数です。各項目の値の有効性は品質一覧で確認してください。",
               "品質件数は項目値の件数です。通信成功と欠測の有無は区別しています。",
               "詳細な年別件数・取得URL・失敗区間はsummary.jsonと各Excelを参照してください。"]
    (root / "取得結果.md").write_text("\n".join(report) + "\n", encoding="utf-8")
    return output
