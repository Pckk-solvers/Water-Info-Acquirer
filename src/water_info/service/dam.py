"""ダム諸量の取得。GUIに依存せず、原データを保存して再開できる。"""

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import pandas as pd

from ..domain.dam import DAM_METRICS, DamRequest, iter_dam_windows
from ..infra.dam_parser import parse_dam_download
from ..infra.http_client import HEADERS, throttled_get

BASE = "https://www1.river.go.jp"


@dataclass
class DamData:
    code: str
    metadata: dict
    frame: pd.DataFrame
    windows: list[dict]
    status: str
    request: DamRequest


def build_dam_url(code, start, end):
    return (f"{BASE}/cgi-bin/DspDamData.exe?KIND=1&ID={code}"
            f"&BGNDATE={start:%Y%m%d}&ENDDATE={end:%Y%m%d}&KAWABOU=NO")


def _download_link(content: bytes, page_url: str):
    try:
        html = content.decode("euc_jp")
    except UnicodeDecodeError:
        html = content.decode("utf-8")
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[href]"):
        url = urljoin(page_url, str(a["href"]))
        parsed = urlparse(url)
        if (parsed.scheme == "https" and parsed.netloc == "www1.river.go.jp"
                and parsed.path.startswith("/dat/dload/") and parsed.path.endswith(".dat")):
            return url
    raise ValueError("ダム結果ページに配布データのリンクがありません")


def fetch_dam_data(code: str, request: DamRequest, *, get=throttled_get,
                   progress=None, should_stop=None) -> DamData:
    if not code.isascii() or not code.isdigit():
        raise ValueError("観測所記号は半角数字で入力してください")
    cache = Path(request.output_dir) / "raw" / code
    cache.mkdir(parents=True, exist_ok=True)
    windows = list(iter_dam_windows(request.start, request.end))
    frames = []
    reports = []
    metadata = {"観測所記号": code}
    cancelled = False
    for index, (start, end) in enumerate(windows, 1):
        page_url = build_dam_url(code, start, end)
        report: dict = {"start": str(start), "end": str(end), "page_url": page_url}
        source = cache / f"{start:%Y%m%d}_{end:%Y%m%d}.dat"
        provenance = source.with_suffix(".json")
        expected = pd.date_range(pd.Timestamp(start) + pd.Timedelta(hours=1),
                                 pd.Timestamp(end) + pd.Timedelta(days=1), freq="h")
        frame = None
        if should_stop and should_stop():
            cancelled = True
        if cancelled:
            report.update(status="cancelled", error="利用者により中止")
        else:
            try:
                cached = False
                meta = {}
                info = {}
                if source.exists() and provenance.exists():
                    try:
                        content = source.read_bytes()
                        info = json.loads(provenance.read_text(encoding="utf-8"))
                        if (not isinstance(info, dict) or info.get("page_url") != page_url
                                or info.get("sha256") != hashlib.sha256(content).hexdigest()):
                            raise ValueError("キャッシュと出典が一致しません")
                        frame, meta = parse_dam_download(content, code, start, end)
                        cached = True
                    except (ValueError, UnicodeError, OSError):
                        frame = None
                if frame is None:
                    page = get(page_url, headers=HEADERS, should_stop=should_stop)
                    download_url = _download_link(page.content, page_url)
                    response = get(download_url, headers=HEADERS, should_stop=should_stop)
                    content = response.content
                    frame, meta = parse_dam_download(content, code, start, end)
                    info = {"page_url": page_url, "download_url": download_url,
                            "retrieved_at": datetime.now().astimezone().isoformat(),
                            "sha256": hashlib.sha256(content).hexdigest()}
                    source.write_bytes(content)
                    provenance.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
                metadata.update(meta)
                report.update(info)
                report.update(status="ok", cached=cached, source_file=str(source), rows=len(frame))
            except Exception as exc:
                if should_stop and should_stop():
                    cancelled = True
                report.update(status="cancelled" if cancelled else "failed", error=str(exc))
        if frame is None:
            frame = pd.DataFrame(index=expected)
        else:
            frame = frame.set_index("datetime").reindex(expected)
        frame.index.name = "datetime"
        frame["station_code"] = code
        source_days = (frame.index - pd.Timedelta(hours=1)).strftime("%Y-%m-%d")
        frame["source_date"] = source_days
        frame["source_time"] = ["24:00" if t.hour == 0 else f"{t.hour:02d}:00" for t in frame.index]
        default_quality = "row_missing" if report["status"] == "ok" else "fetch_failed"
        for metric in DAM_METRICS:
            for suffix in ("", "_raw", "_attribute", "_quality"):
                col = metric.key + suffix
                if col not in frame:
                    frame[col] = None
            frame[metric.key] = pd.to_numeric(frame[metric.key], errors="coerce").astype("float64")
            frame[f"{metric.key}_quality"] = frame[f"{metric.key}_quality"].fillna(default_quality)
            for suffix in ("_raw", "_attribute"):
                frame[metric.key + suffix] = frame[metric.key + suffix].fillna("")
        report["missing_rows"] = int(frame[f"{DAM_METRICS[0].key}_quality"].isin(
            ["row_missing", "fetch_failed"]).sum())
        frames.append(frame.reset_index())
        reports.append(report)
        if progress:
            progress(index, len(windows), report)
    status = "cancelled" if cancelled else (
        "partial" if any(r["status"] != "ok" for r in reports) else "complete")
    return DamData(code, metadata, pd.concat(frames, ignore_index=True), reports, status, request)
