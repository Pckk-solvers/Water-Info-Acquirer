"""取得画面の両対象を、Tkに依存せず共通の進捗・結果で実行する。"""

from dataclasses import dataclass
from pathlib import Path

from ..domain.models import WaterInfoRequest
from ..domain.dam import DAM_METRICS, QUALITY_LABELS, DamRequest, iter_dam_windows
from ..infra.dam_writer import quality_summary, write_dam_outputs
from .dam import fetch_dam_data


@dataclass(frozen=True)
class AcquisitionPlan:
    codes: tuple[str, ...]
    request: WaterInfoRequest | DamRequest
    output_dir: Path


@dataclass(frozen=True)
class AcquisitionProgress:
    current: int
    total: int
    code: str
    message: str


@dataclass(frozen=True)
class AcquisitionResult:
    code: str
    station_name: str
    status: str
    summary: str
    detail: str
    output_dir: Path
    excel_path: Path | None = None


def units_per_station(request):
    if isinstance(request, DamRequest):
        return len(list(iter_dam_windows(request.start, request.end)))
    p = request.period
    years = int(p.year_end) - int(p.year_start)
    if request.options.use_daily:
        return years + 1
    return years * 12 + int(p.month_end[:-1]) - int(p.month_start[:-1]) + 2


def run_acquisition(plan: AcquisitionPlan, *, fetch_hourly, fetch_daily,
                    should_stop, on_progress, on_result):
    per_station = units_per_station(plan.request)
    total = per_station * len(plan.codes)
    for index, code in enumerate(plan.codes):
        if should_stop():
            for skipped in plan.codes[index:]:
                on_result(AcquisitionResult(skipped, "", "cancelled", "未実行", "中止により未実行です。", plan.output_dir))
            break
        base = index * per_station
        on_progress(AcquisitionProgress(base, total, code, "取得を開始しています"))
        try:
            if isinstance(plan.request, DamRequest):
                request = plan.request
                folder = plan.output_dir / f"{code}_{request.start:%Y%m%d}_{request.end:%Y%m%d}"
                actual = DamRequest(request.start, request.end, folder, request.export_parquet)

                def dam_progress(n, _total, report):
                    on_progress(AcquisitionProgress(base + n, total, code,
                        f"{report['start']} 〜 {report['end']} を確認しました"))

                data = fetch_dam_data(code, actual, should_stop=should_stop, progress=dam_progress)
                outputs = write_dam_outputs(data, progress=lambda message: on_progress(
                    AcquisitionProgress(base + per_station, total, code, message)))
                counts = quality_summary(data.frame)
                nonnormal = sum(v for values in counts.values() for q, v in values.items() if q != "normal")
                status = data.status if data.status != "complete" or nonnormal == 0 else "warning"
                details = [f"時間行数: {len(data.frame):,}", "品質件数（項目値）:"]
                for m in DAM_METRICS:
                    details.append(m.label + ": " + " / ".join(
                        f"{label} {counts[m.key][q]:,}" for q, label in QUALITY_LABELS.items()
                        if q == "normal" or counts[m.key][q]))
                failed = [r for r in data.windows if r["status"] != "ok"]
                details.extend(f"未取得 {r['start']}〜{r['end']}: {r.get('error', '')}" for r in failed)
                result = AcquisitionResult(code, data.metadata.get("観測所名", ""), status,
                    f"{len(data.frame):,}時間行 / 欠測等 {nonnormal:,}項目値", "\n".join(details),
                    folder, Path(outputs["excel"][0]))
            else:
                normal = plan.request
                p = normal.period
                done = 0
                station = ""

                def normal_progress(*, increment=True, station_name=None):
                    nonlocal done, station
                    if station_name:
                        station = station_name
                    if increment:
                        done = min(done + 1, per_station)
                    on_progress(AcquisitionProgress(base + done, total, code, f"{station} の取得・整理中"))

                fetch = fetch_daily if normal.options.use_daily else fetch_hourly
                path = fetch(code, p.year_start, p.year_end, p.month_start, p.month_end,
                    normal.mode_type, single_sheet=normal.options.single_sheet,
                    export_parquet=normal.options.export_parquet, output_dir=plan.output_dir,
                    should_stop=should_stop, progress_callback=normal_progress)
                result = AcquisitionResult(code, station, "complete", "Excelを保存しました",
                    "取得処理・出力が完了しました。値の欠測状況は出力したExcelのsummaryで確認してください。",
                    plan.output_dir, Path(path))
            on_result(result)
        except Exception as exc:
            cancelled = should_stop()
            on_result(AcquisitionResult(code, "", "cancelled" if cancelled else "failed",
                "中止しました" if cancelled else "取得・出力に失敗しました", str(exc), plan.output_dir))
        on_progress(AcquisitionProgress(base + per_station, total, code, "観測所の処理が終了しました"))
