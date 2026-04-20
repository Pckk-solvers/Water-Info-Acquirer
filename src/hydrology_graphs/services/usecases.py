from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import pandas as pd
from ..domain.logic import (
    annual_max_by_year,
    annual_max_series,
    evaluate_event_series_status,
    event_capture_window_bounds,
    ensure_graph_type_supported,
    extract_event_series,
    has_min_years,
    is_event_graph,
    required_metric_interval,
)
from ..domain.models import GraphTarget, ThresholdRecord
from ..io.parquet_store import ParquetCatalog, scan_parquet_catalog, scan_parquet_station_index
from ..io.png_writer import write_png
from ..io.style_store import StyleLoadResult, load_style, style_key_for_target
from ..io.threshold_store import ThresholdLoadResult, load_thresholds, thresholds_for_key
from ..render.plotter import render_graph_png
from .dto import (
    BatchRunInput,
    BatchRunItemResult,
    BatchRunResult,
    BatchSummary,
    BatchTarget,
    PrecheckInput,
    PrecheckItem,
    PrecheckResult,
    PrecheckSummary,
    PreviewInput,
    PreviewResult,
)

"""サービス層のユースケース実装。

ここでは、Parquet のスキャン結果をもとに、検証・プレビュー・
バッチ描画をまとめて制御する。
"""

REASON_CONTRACT_ERROR = "contract_error"
REASON_MISSING_TIMESERIES = "missing_timeseries"
REASON_MISSING_WITH_WARNING = "missing_with_warning"
REASON_INSUFFICIENT_YEARS = "insufficient_years"
REASON_THRESHOLD_NOT_FOUND = "threshold_not_found"
REASON_STYLE_ERROR = "style_error"
REASON_RENDER_ERROR = "render_error"


@dataclass(slots=True)
class UsecaseError(Exception):
    """サービス内部で使うエラー。reason_code を必ず持つ。"""

    reason_code: str
    message: str


class HydrologyGraphService:
    """UI から呼ばれる窓口クラス。"""

    def scan_catalog(self, parquet_dir: str | Path) -> ParquetCatalog:
        """Parquet ディレクトリを走査する。"""

        return scan_parquet_catalog(parquet_dir)

    def scan_station_index(self, parquet_dir: str | Path) -> ParquetCatalog:
        """観測所一覧表示向けに軽量走査する。"""

        return scan_parquet_station_index(parquet_dir)

    def precheck(self, data: PrecheckInput) -> PrecheckResult:
        return precheck_graph_targets(data)

    def precheck_with_catalog(
        self,
        *,
        catalog: ParquetCatalog,
        data: PrecheckInput,
        threshold_result: ThresholdLoadResult | None = None,
    ) -> PrecheckResult:
        """既に走査済みの catalog を使って実行前検証する。"""

        return precheck_graph_targets_with_catalog(
            catalog=catalog,
            data=data,
            threshold_result=threshold_result,
        )

    def preview(self, data: PreviewInput) -> PreviewResult:
        return preview_graph_target(data)

    def preview_with_catalog(
        self,
        *,
        catalog: ParquetCatalog,
        data: PreviewInput,
        threshold_result: ThresholdLoadResult | None = None,
    ) -> PreviewResult:
        """既に走査済みの catalog を使ってプレビューを生成する。"""

        return preview_graph_target_with_catalog(
            catalog=catalog,
            data=data,
            threshold_result=threshold_result,
        )

    def run_batch(
        self,
        data: BatchRunInput,
        *,
        stop_requested: Callable[[], bool] | None = None,
    ) -> BatchRunResult:
        if stop_requested is None:
            stop_requested = data.should_stop
        return run_graph_batch(data, stop_requested=stop_requested)

    def _render_target(
        self,
        catalog: ParquetCatalog,
        target: GraphTarget,
        style: dict,
        threshold_file_path: str | None,
    ) -> bytes:
        return _render_target_bytes(
            catalog,
            target,
            style,
            threshold_file_path=threshold_file_path,
        )


def precheck_graph_targets(data: PrecheckInput) -> PrecheckResult:
    """条件設定タブで使う事前検証を実行する。"""

    catalog = scan_parquet_catalog(data.parquet_dir)
    return precheck_graph_targets_with_catalog(catalog=catalog, data=data)


def precheck_graph_targets_with_catalog(
    *,
    catalog: ParquetCatalog,
    data: PrecheckInput,
    threshold_result: ThresholdLoadResult | None = None,
) -> PrecheckResult:
    """走査済み catalog を使って実行前検証を実行する。"""

    if threshold_result is None and data.threshold_file_path:
        threshold_result = load_thresholds(data.threshold_file_path)

    # UI から来る station_pairs を優先し、旧入力の station_keys/sources も拾えるようにする。
    station_pairs = _resolve_station_pairs(data, catalog)

    items: list[PrecheckItem] = []
    ok_targets = 0
    warn_targets = 0
    ng_targets = 0

    for source, station_key in station_pairs:
        for graph_type in data.graph_types:
            # 未知のグラフ種別は早期に除外して、以降の判定を無駄にしない。
            if not ensure_graph_type_supported(graph_type):
                ng_targets += 1
                items.append(
                    PrecheckItem(
                        target_id=f"{source}:{station_key}:{graph_type}:annual",
                        source=source,
                        station_key=station_key,
                        graph_type=graph_type,
                        base_datetime=None,
                        status="ng",
                        event_window_days=None,
                        reason_code=REASON_CONTRACT_ERROR,
                        reason_message="未対応のgraph_typeです。",
                    )
                )
                continue

            if is_event_graph(graph_type):
                # イベント系は基準日ごとに独立した描画対象になる。
                per_graph = data.event_window_days_by_graph.get(graph_type, [])
                window_days_list = _normalized_event_window_days_list(per_graph or data.event_window_days_list)
                if not window_days_list:
                    ng_targets += 1
                    items.append(
                        PrecheckItem(
                            target_id=f"{source}:{station_key}:{graph_type}:none",
                            source=source,
                            station_key=station_key,
                            graph_type=graph_type,
                            base_datetime=None,
                            status="ng",
                            event_window_days=None,
                            reason_code=REASON_CONTRACT_ERROR,
                            reason_message="イベント窓が未選択です。",
                        )
                    )
                    continue
                if not data.base_dates:
                    for window_days in window_days_list:
                        ng_targets += 1
                        items.append(
                            PrecheckItem(
                                target_id=f"{source}:{station_key}:{graph_type}:none:{window_days}day",
                                source=source,
                                station_key=station_key,
                                graph_type=graph_type,
                                base_datetime=None,
                                status="ng",
                                event_window_days=window_days,
                                reason_code=REASON_MISSING_TIMESERIES,
                                reason_message="基準日が未指定です。",
                            )
                        )
                    continue
                for base_date_text in data.base_dates:
                    parsed = _parse_date(base_date_text)
                    if parsed is None:
                        for window_days in window_days_list:
                            ng_targets += 1
                            items.append(
                                PrecheckItem(
                                    target_id=f"{source}:{station_key}:{graph_type}:{base_date_text}:{window_days}day",
                                    source=source,
                                    station_key=station_key,
                                    graph_type=graph_type,
                                    base_datetime=base_date_text,
                                    status="ng",
                                    event_window_days=window_days,
                                    reason_code=REASON_CONTRACT_ERROR,
                                    reason_message="基準日の形式は YYYY-MM-DD で指定してください。",
                                )
                            )
                        continue
                    # 3/5日が同時選択のときは、5日結果を可能な範囲で再利用して重複計算を減らす。
                    evaluated_by_window = _evaluate_event_windows(
                        frame=catalog.data,
                        source=source,
                        station_key=station_key,
                        graph_type=graph_type,
                        base_datetime=parsed.isoformat(),
                        window_days_list=window_days_list,
                        terminal_padding_hours=_event_padding_hours(data.event_window_terminal_padding),
                        threshold_result=threshold_result,
                    )
                    for window_days in window_days_list:
                        status, reason_code, reason_message, _ = evaluated_by_window[window_days]
                        if status == "ok":
                            ok_targets += 1
                        elif status == "warn":
                            warn_targets += 1
                        else:
                            ng_targets += 1
                        items.append(
                            PrecheckItem(
                                target_id=f"{source}:{station_key}:{graph_type}:{parsed.isoformat()}:{window_days}day",
                                source=source,
                                station_key=station_key,
                                graph_type=graph_type,
                                base_datetime=parsed.isoformat(),
                                status=status,
                                event_window_days=window_days,
                                reason_code=reason_code,
                                reason_message=reason_message,
                            )
                        )
            else:
                # 年最大系は基準日を持たないため、station + graph_type 単位で判定する。
                try:
                    status, reason_code, reason_message, _ = _evaluate_target(
                        catalog.data,
                        source=source,
                        station_key=station_key,
                        graph_type=graph_type,
                        base_datetime=None,
                        event_window_days=None,
                        terminal_padding_hours=0,
                        threshold_result=threshold_result,
                    )
                except UsecaseError as exc:
                    status = "ng"
                    reason_code = exc.reason_code
                    reason_message = exc.message
                if status == "ok":
                    ok_targets += 1
                elif status == "warn":
                    warn_targets += 1
                else:
                    ng_targets += 1
                items.append(
                    PrecheckItem(
                        target_id=f"{source}:{station_key}:{graph_type}:annual",
                        source=source,
                        station_key=station_key,
                        graph_type=graph_type,
                        base_datetime=None,
                        status=status,
                        event_window_days=None,
                        reason_code=reason_code,
                        reason_message=reason_message,
                    )
                )

    issues = list(catalog.warnings)
    issues.extend(f"{path}: {', '.join(errors)}" for path, errors in catalog.invalid_files.items())
    if threshold_result is not None:
        issues.extend(threshold_result.warnings)
    return PrecheckResult(
        summary=PrecheckSummary(
            total_targets=len(items),
            ok_targets=ok_targets,
            warn_targets=warn_targets,
            ng_targets=ng_targets,
        ),
        items=items,
        issues=issues,
    )


def _resolve_station_pairs(
    data: PrecheckInput,
    catalog: ParquetCatalog,
) -> list[tuple[str, str]]:
    """Precheck対象の観測所ペアを解決する。

    優先順位:
    1. station_pairs（新仕様）
    2. station_keys + sources（互換）
    """

    if data.station_pairs:
        # 新しい入力形式はそのまま受け取り、重複だけ落とす。
        seen: set[tuple[str, str]] = set()
        ordered: list[tuple[str, str]] = []
        for source, station_key in data.station_pairs:
            pair = (source, station_key)
            if pair in seen:
                continue
            seen.add(pair)
            ordered.append(pair)
        return ordered

    station_keys = list(dict.fromkeys(data.station_keys))
    if not station_keys:
        return []
    sources = data.sources
    if sources is None:
        # source 未指定なら、カタログから station_key が存在する source を推定する。
        sources = sorted({source for source, station_key, _ in catalog.stations if station_key in station_keys})
    if not sources:
        sources = ["jma", "water_info"]
    return [(source, station_key) for source in sources for station_key in station_keys]


def _normalized_event_window_days_list(raw: list[int]) -> list[int]:
    """イベント窓の入力を 3/5 日の重複なし順序で正規化する。"""

    ordered: list[int] = []
    for day in raw:
        if day not in (3, 5):
            continue
        if day in ordered:
            continue
        ordered.append(day)
    return ordered


def preview_graph_target(data: PreviewInput) -> PreviewResult:
    """スタイル調整タブで使う単体プレビューを生成する。"""

    catalog = scan_parquet_catalog(data.parquet_dir)
    return preview_graph_target_with_catalog(catalog=catalog, data=data)


def preview_graph_target_with_catalog(
    *,
    catalog: ParquetCatalog,
    data: PreviewInput,
    threshold_result: ThresholdLoadResult | None = None,
) -> PreviewResult:
    """走査済み catalog を使って単体プレビューを生成する。"""

    style_result = load_style(data.style_json_path, payload=data.style_payload)
    if _has_style_error(style_result):
        # スタイル JSON が壊れているなら、描画に進まずここで止める。
        return PreviewResult(
            status="error",
            reason_code=REASON_STYLE_ERROR,
            reason_message="; ".join(style_result.warnings),
            image_bytes_png=None,
        )

    if threshold_result is None and data.threshold_file_path:
        threshold_result = load_thresholds(data.threshold_file_path)
    target = GraphTarget(
        source=data.source,
        station_key=data.station_key,
        graph_type=data.graph_type,  # type: ignore[arg-type]
        base_date=_parse_date(data.base_datetime) if data.base_datetime else None,
        event_window_days=data.event_window_days,
    )
    target2 = None
    if data.source2 and data.station_key2:
        target2 = GraphTarget(
            source=data.source2,
            station_key=data.station_key2,
            graph_type=data.graph_type,  # type: ignore[arg-type]
            base_date=_parse_date(data.base_datetime) if data.base_datetime else None,
            event_window_days=data.event_window_days,
        )

    try:
        # プレビューは本番実行と同じ描画パスを通して、見た目のズレをなくす。
        png, target_status, target_reason = _render_target_bytes_with_status(
            catalog,
            target,
            style_result.style,
            target2=target2,
            threshold_file_path=data.threshold_file_path,
            threshold_result=threshold_result,
            terminal_padding_hours=_event_padding_hours(data.event_window_terminal_padding),
            time_display_mode=data.time_display_mode,
        )
        return PreviewResult(
            status="success",
            reason_code=REASON_MISSING_WITH_WARNING if target_status == "warn" else None,
            reason_message=target_reason if target_status == "warn" else None,
            image_bytes_png=png,
        )
    except UsecaseError as exc:
        return PreviewResult(
            status="error",
            reason_code=exc.reason_code,
            reason_message=exc.message,
            image_bytes_png=None,
        )
    except Exception as exc:  # noqa: BLE001
        return PreviewResult(
            status="error",
            reason_code=REASON_RENDER_ERROR,
            reason_message=str(exc),
            image_bytes_png=None,
        )


def run_graph_batch(
    data: BatchRunInput,
    *,
    stop_requested: Callable[[], bool] | None = None,
) -> BatchRunResult:
    """複数対象のバッチ描画を実行する。"""

    catalog = scan_parquet_catalog(data.parquet_dir)
    style_result = load_style(data.style_json_path, payload=data.style_payload)
    if _has_style_error(style_result):
        # 全件共通のスタイル不正は、個別対象の問題ではないので一括失敗にする。
        items = [
            BatchRunItemResult(
                target_id=_batch_target_id(t),
                status="failed",
                reason_code=REASON_STYLE_ERROR,
                reason_message="; ".join(style_result.warnings),
            )
            for t in data.targets
        ]
        return _finalize_batch(items)

    threshold_result = load_thresholds(data.threshold_file_path) if data.threshold_file_path else None
    items: list[BatchRunItemResult] = []
    output_dir = Path(data.output_dir)

    for index, target in enumerate(data.targets, start=1):
        if stop_requested is not None and stop_requested():
            # 停止要求後は、まだ着手していない対象だけを skipped にする。
            for pending in data.targets[index - 1 :]:
                items.append(
                    BatchRunItemResult(
                        target_id=_batch_target_id(pending),
                        status="skipped",
                        reason_message="停止要求により未着手対象をスキップしました。",
                    )
                )
            break

        try:
            # 1件ずつ描画して保存する。途中失敗しても次の対象へ進む。
            png, target_status, target_reason = _render_target_bytes_with_status(
                catalog,
                GraphTarget(
                    source=target.source,
                    station_key=target.station_key,
                    graph_type=target.graph_type,  # type: ignore[arg-type]
                    base_date=_parse_date(target.base_datetime) if target.base_datetime else None,
                    event_window_days=target.event_window_days,
                ),
                style_result.style,
                threshold_file_path=data.threshold_file_path,
                threshold_result=threshold_result,
                terminal_padding_hours=_event_padding_hours(data.event_window_terminal_padding),
                time_display_mode=data.time_display_mode,
            )
            output_path = _build_output_path(output_dir, target)
            write_png(output_path, png)
            items.append(
                BatchRunItemResult(
                    target_id=_batch_target_id(target),
                    status="success",
                    reason_code=REASON_MISSING_WITH_WARNING if target_status == "warn" else None,
                    reason_message=target_reason if target_status == "warn" else None,
                    output_path=str(output_path),
                )
            )
        except UsecaseError as exc:
            # データ不足や年数不足は再実行可能なので skipped 寄りで扱う。
            status = (
                "skipped"
                if exc.reason_code in {REASON_MISSING_TIMESERIES, REASON_INSUFFICIENT_YEARS}
                else "failed"
            )
            items.append(
                BatchRunItemResult(
                    target_id=_batch_target_id(target),
                    status=status,
                    reason_code=exc.reason_code,
                    reason_message=exc.message,
                )
            )
        except Exception as exc:  # noqa: BLE001
            items.append(
                BatchRunItemResult(
                    target_id=_batch_target_id(target),
                    status="failed",
                    reason_code=REASON_RENDER_ERROR,
                    reason_message=str(exc),
                )
            )

    return _finalize_batch(items)


def _evaluate_target(
    frame: pd.DataFrame,
    *,
    source: str,
    station_key: str,
    graph_type: str,
    base_datetime: str | None,
    event_window_days: int | None,
    terminal_padding_hours: int,
    threshold_result: ThresholdLoadResult | None,
) -> tuple[str, str | None, str | None, pd.DataFrame | None]:
    """1対象分の検証を行い、描画に使う DataFrame を返す。"""

    if not ensure_graph_type_supported(graph_type):
        return "ng", REASON_CONTRACT_ERROR, "未対応のgraph_typeです。", None

    metric, interval = required_metric_interval(graph_type)
    # event 系は interval を固定で要求し、annual 系は metric だけ合わせる。
    subset = _select_frame(
        frame,
        source=source,
        station_key=station_key,
        metric=metric,
        interval=interval if is_event_graph(graph_type) else None,
    )
    if subset.empty:
        if frame.empty:
            return "ng", REASON_CONTRACT_ERROR, "Parquet契約違反または空データです。", None
        return "ng", REASON_MISSING_TIMESERIES, "該当時系列が見つかりません。", None

    if is_event_graph(graph_type):
        # イベント系は基準日と窓長が必須。
        if base_datetime is None:
            return "ng", REASON_CONTRACT_ERROR, "基準日が必要です。", None
        if event_window_days not in (3, 5):
            return "ng", REASON_CONTRACT_ERROR, "イベント系グラフは3日または5日窓が必要です。", None
        parsed = _parse_date(base_datetime)
        if parsed is None:
            return "ng", REASON_CONTRACT_ERROR, "基準日の形式は YYYY-MM-DD で指定してください。", None
        sliced = extract_event_series(
            subset,
            parsed,
            event_window_days,
            terminal_padding_hours=terminal_padding_hours,
        )
        event_status, reason = evaluate_event_series_status(
            sliced,
            parsed,
            event_window_days,
            terminal_padding_hours=terminal_padding_hours,
        )
        if event_status == "ng":
            return "ng", REASON_MISSING_TIMESERIES, reason or "イベント窓に有効データがありません。", None
        thresholds = _thresholds_for_target(threshold_result, source, station_key, graph_type)
        if threshold_result is not None and not thresholds:
            return "ng", REASON_THRESHOLD_NOT_FOUND, "対応する基準線が見つかりません。", None
        if event_status == "warn":
            return "warn", REASON_MISSING_WITH_WARNING, reason or "欠測あり（描画継続）", sliced.reset_index(drop=True)
        return "ok", None, None, sliced.reset_index(drop=True)

    annual = annual_max_series(subset)
    # 年最大系は十分な年数がないと意味が薄いので、まず年数だけで足切りする。
    if not has_min_years(annual, 10):
        return "ng", REASON_INSUFFICIENT_YEARS, "年最大グラフは10年以上のデータが必要です。", None
    annual_df = annual_max_by_year(subset)
    if annual_df.empty:
        return "ng", REASON_MISSING_TIMESERIES, "年最大算出結果が空です。", None
    thresholds = _thresholds_for_target(threshold_result, source, station_key, graph_type)
    if threshold_result is not None and not thresholds:
        return "ng", REASON_THRESHOLD_NOT_FOUND, "対応する基準線が見つかりません。", None
    return "ok", None, None, annual_df.reset_index(drop=True)


def _evaluate_event_windows(
    *,
    frame: pd.DataFrame,
    source: str,
    station_key: str,
    graph_type: str,
    base_datetime: str,
    window_days_list: list[int],
    terminal_padding_hours: int,
    threshold_result: ThresholdLoadResult | None,
) -> dict[int, tuple[str, str | None, str | None, pd.DataFrame | None]]:
    """イベント窓(3/5)の評価を行い、可能な範囲で5日結果を再利用する。"""

    ordered = _normalized_event_window_days_list(window_days_list)
    results: dict[int, tuple[str, str | None, str | None, pd.DataFrame | None]] = {}
    if not ordered:
        return results

    def _safe_eval(window_days: int) -> tuple[str, str | None, str | None, pd.DataFrame | None]:
        try:
            return _evaluate_target(
                frame,
                source=source,
                station_key=station_key,
                graph_type=graph_type,
                base_datetime=base_datetime,
                event_window_days=window_days,
                terminal_padding_hours=terminal_padding_hours,
                threshold_result=threshold_result,
            )
        except UsecaseError as exc:
            return "ng", exc.reason_code, exc.message, None

    # 3日・5日同時選択時は5日を先に評価し、OKなら中心3日を再利用する。
    if 3 in ordered and 5 in ordered:
        result_5 = _safe_eval(5)
        results[5] = result_5
        status_5, _rc_5, _rm_5, draw_df_5 = result_5
        if status_5 in {"ok", "warn"} and draw_df_5 is not None:
            parsed = _parse_date(base_datetime)
            if parsed is not None:
                start_3, end_3 = event_capture_window_bounds(parsed, 3, terminal_padding_hours)
                time_col = "period_end_at" if "period_end_at" in draw_df_5.columns else "observed_at"
                observed = pd.to_datetime(draw_df_5[time_col], errors="coerce")
                mask = (observed >= pd.Timestamp(start_3)) & (observed < pd.Timestamp(end_3))
                draw_df_3 = draw_df_5.loc[mask].copy()
                status_3, reason_3 = evaluate_event_series_status(
                    draw_df_3,
                    parsed,
                    3,
                    terminal_padding_hours=terminal_padding_hours,
                )
                if status_3 == "ok":
                    results[3] = "ok", None, None, draw_df_3.reset_index(drop=True)
                elif status_3 == "warn":
                    results[3] = "warn", REASON_MISSING_WITH_WARNING, reason_3 or "欠測あり（描画継続）", draw_df_3.reset_index(drop=True)
                else:
                    results[3] = _safe_eval(3)
            else:
                results[3] = _safe_eval(3)
        else:
            # 5日がNGでも3日は成立する可能性があるため、3日は独立評価する。
            results[3] = _safe_eval(3)

    for day in ordered:
        if day in results:
            continue
        results[day] = _safe_eval(day)
    return results


def _render_target_bytes_with_status(
    catalog: ParquetCatalog,
    target: GraphTarget,
    style: dict,
    *,
    target2: GraphTarget | None = None,
    threshold_file_path: str | None,
    threshold_result: ThresholdLoadResult | None = None,
    terminal_padding_hours: int = 0,
    time_display_mode: str = "datetime",
) -> tuple[bytes, str, str | None]:
    """1対象分（または比較ペア）の PNG を生成し、評価状態（ok/warn）を返す。"""

    effective_threshold_result = (
        threshold_result
        if threshold_result is not None
        else load_thresholds(threshold_file_path)
        if threshold_file_path
        else None
    )
    # メイン系列の評価
    status, reason_code, reason_message, draw_df = _evaluate_target(
        catalog.data,
        source=target.source,
        station_key=target.station_key,
        graph_type=target.graph_type,
        base_datetime=target.base_date.isoformat() if target.base_date else None,
        event_window_days=target.event_window_days,
        terminal_padding_hours=terminal_padding_hours,
        threshold_result=effective_threshold_result,
    )
    if status not in {"ok", "warn"} or draw_df is None:
        raise UsecaseError(reason_code or REASON_CONTRACT_ERROR, reason_message or "描画対象が見つかりません。")

    # 比較系列の評価
    draw_df2 = None
    station_name2 = None
    if target2 is not None:
        # 比較対象はNGであってもメインの描画を優先するため、例外を投げずにNone扱いとする。
        status2, _, _, draw_df2 = _evaluate_target(
            catalog.data,
            source=target2.source,
            station_key=target2.station_key,
            graph_type=target2.graph_type,
            base_datetime=target2.base_date.isoformat() if target2.base_date else None,
            event_window_days=target2.event_window_days,
            terminal_padding_hours=terminal_padding_hours,
            threshold_result=effective_threshold_result,
        )
        if status2 in {"ok", "warn"} and draw_df2 is not None:
            station_name2 = _resolve_station_name(catalog, target2.station_key)

    thresholds = _thresholds_for_target(
        effective_threshold_result,
        target.source,
        target.station_key,
        target.graph_type,
    )
    station_name = _resolve_station_name(catalog, target.station_key)
    style_key = style_key_for_target(target.graph_type, target.event_window_days)
    if style_key is None:
        raise UsecaseError(REASON_CONTRACT_ERROR, "スタイル対象キーを解決できません。")
    graph_styles = style.get("graph_styles")
    if not isinstance(graph_styles, dict):
        raise UsecaseError(REASON_STYLE_ERROR, "style.graph_styles が不正です。")
    graph_style = graph_styles.get(style_key)
    if not isinstance(graph_style, dict):
        raise UsecaseError(REASON_STYLE_ERROR, f"style.graph_styles.{style_key} が見つかりません。")
    try:
        # render 層には描画だけを渡し、保存や I/O は外側で分離する。
        png = render_graph_png(
            graph_type=target.graph_type,
            station_name=station_name,
            df=draw_df,
            graph_style=graph_style,
            thresholds=thresholds,
            time_display_mode=time_display_mode,
            station_name2=station_name2,
            df2=draw_df2,
        )
        return png, status, reason_message
    except Exception as exc:  # noqa: BLE001
        raise UsecaseError(REASON_RENDER_ERROR, str(exc)) from exc


def _render_target_bytes(
    catalog: ParquetCatalog,
    target: GraphTarget,
    style: dict,
    *,
    threshold_file_path: str | None,
    threshold_result: ThresholdLoadResult | None = None,
    terminal_padding_hours: int = 0,
    time_display_mode: str = "datetime",
) -> bytes:
    """1対象分の PNG を生成する。"""

    png, _status, _reason = _render_target_bytes_with_status(
        catalog,
        target,
        style,
        threshold_file_path=threshold_file_path,
        threshold_result=threshold_result,
        terminal_padding_hours=terminal_padding_hours,
        time_display_mode=time_display_mode,
    )
    return png


def _has_style_error(result: StyleLoadResult) -> bool:
    """スタイル読込結果に致命的エラーがあるかを判定する。"""

    return any(message.startswith("error:") or message.startswith("style_file_not_found:") for message in result.warnings)


def _build_output_path(output_dir: Path, target: BatchTarget) -> Path:
    """出力ファイルの配置パスを組み立てる。"""

    base = target.base_datetime or "annual"
    if target.base_datetime and target.event_window_days in (3, 5):
        return output_dir / target.station_key / target.graph_type / base / f"{target.event_window_days}day" / "graph.png"
    return output_dir / target.station_key / target.graph_type / base / "graph.png"


def _finalize_batch(items: list[BatchRunItemResult]) -> BatchRunResult:
    """バッチ結果を集計して返す。"""

    summary = BatchSummary(
        total=len(items),
        success=sum(1 for item in items if item.status == "success"),
        failed=sum(1 for item in items if item.status == "failed"),
        skipped=sum(1 for item in items if item.status == "skipped"),
    )
    return BatchRunResult(summary=summary, items=items, issues=[])


def _parse_date(value: str | None) -> date | None:
    """文字列の日付を date に変換する。"""

    if value is None:
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if getattr(parsed, "tzinfo", None) is not None:
        parsed = parsed.tz_convert(None)
    return parsed.date()


def _event_padding_hours(enabled: bool) -> int:
    return 1 if enabled else 0


def _resolve_station_name(catalog: ParquetCatalog, station_key: str) -> str:
    """catalog から観測所名を引く。"""

    for _source, key, name in catalog.stations:
        if key == station_key:
            # 同じ station_key が複数 source に出る場合は、先に見つかった名称を採用する。
            return name
    return ""


def _batch_target_id(target: BatchTarget) -> str:
    """バッチ対象の表示用 ID を返す。"""

    base = target.base_datetime or "annual"
    if target.base_datetime and target.event_window_days in (3, 5):
        return f"{target.source}:{target.station_key}:{target.graph_type}:{base}:{target.event_window_days}day"
    return f"{target.source}:{target.station_key}:{target.graph_type}:{base}"


def _thresholds_for_target(
    result: ThresholdLoadResult | None,
    source: str,
    station_key: str,
    graph_type: str,
) -> list[ThresholdRecord]:
    """指定対象に対応する基準線一覧を返す。"""

    if result is None:
        return []
    return thresholds_for_key(
        result.lines,
        source=source,
        station_key=station_key,
        graph_type=graph_type,
    )


def _select_frame(
    frame: pd.DataFrame,
    *,
    source: str,
    station_key: str,
    metric: str,
    interval: str | None,
) -> pd.DataFrame:
    """条件に一致する時系列だけを切り出す。"""

    mask = (
        (frame["source"] == source)
        & (frame["station_key"] == station_key)
        & (frame["metric"] == metric)
    )
    if interval is not None:
        mask = mask & (frame["interval"] == interval)
    return frame.loc[mask].copy()
