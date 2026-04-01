from __future__ import annotations

from datetime import datetime
from pathlib import Path

from hydrology_graphs.services import PreviewInput


def render_preview(app, *, silent_json_error: bool = False) -> None:
    """プレビュー画像を再生成して表示する。"""

    if app._scan_running:
        return
    if app._catalog is None:
        app.preview_message.set("先にParquetをスキャンしてください。")
        return
    built = _build_preview_input(app, silent_json_error=silent_json_error)
    if built is None:
        return
    preview_input, threshold_file = built
    app._preview_pending = {"input": preview_input, "threshold_file": threshold_file}
    app.preview_message.set("プレビュー更新中...")
    app._start_preview_worker_if_needed()


def export_preview_sample(app) -> None:
    """現在のプレビュー対象をPNG出力する（開発者モード専用）。"""

    if not getattr(app, "developer_mode", False):
        app.preview_message.set("開発者モード専用機能です。")
        return
    if app._catalog is None:
        app.preview_message.set("先にParquetをスキャンしてください。")
        return
    built = _build_preview_input(app, silent_json_error=False, for_export=True)
    if built is None:
        return
    preview_input, threshold_file = built
    threshold_result = app._load_thresholds_cached(threshold_file)
    try:
        result = app.service.preview_with_catalog(
            catalog=app._catalog,
            data=preview_input,
            threshold_result=threshold_result,
        )
    except Exception as exc:  # noqa: BLE001
        app.preview_message.set(f"サンプル出力エラー: {exc}")
        return
    if result.status != "success" or result.image_bytes_png is None:
        app.preview_message.set(result.reason_message or "サンプル出力に失敗しました。")
        return
    out_path = _build_sample_output_path(
        graph_type=preview_input.graph_type,
        event_window_days=preview_input.event_window_days,
        station_key=preview_input.station_key,
        base_datetime=preview_input.base_datetime,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(result.image_bytes_png)
    app.preview_message.set(f"サンプル出力完了: {out_path}")
    app._append_log(f"[SAMPLE] exported {out_path}")


def _build_preview_input(
    app,
    *,
    silent_json_error: bool,
    for_export: bool = False,
) -> tuple[PreviewInput, str | None] | None:
    """現在UI選択から PreviewInput を組み立てる。"""

    # ボタン実行時はフォーム未確定入力も反映してからJSONを読む。
    payload_from_editor = app._style_from_editor(silent=silent_json_error)
    if payload_from_editor is not None:
        app._style_payload = payload_from_editor
    if not app._apply_style_form_values():
        return None
    app._set_style_text_from_payload()
    app._push_style_history(app._style_payload)

    station_token = app.preview_target_station.get().strip()
    if not station_token:
        return None
    station_pair = app._preview_station_display_to_pair.get(station_token)
    if station_pair is None:
        app.preview_message.set("観測所の指定が不正です。")
        return None
    source, station_key = station_pair
    style_key = app._current_style_graph_key()
    if ":" in style_key and style_key.endswith("day"):
        graph_type, day_suffix = style_key.split(":", 1)
        try:
            event_window_days = int(day_suffix[:-3])
        except Exception:  # noqa: BLE001
            app.preview_message.set("スタイル対象キーが不正です。")
            return None
        base_date = app.preview_target_date.get().strip() or None
        if not base_date:
            app.preview_message.set("イベント系グラフは基準日を指定してください。")
            return None
    else:
        graph_type = style_key
        event_window_days = None
        base_date = None

    preview_payload = app._style_payload if for_export else app._build_preview_style_payload(app._style_payload)
    threshold_file = app.threshold_path.get().strip() or None
    preview_input = PreviewInput(
        parquet_dir=app.parquet_dir.get().strip(),
        threshold_file_path=threshold_file,
        style_json_path=app._style_json_path,
        style_payload=preview_payload,
        source=source,
        station_key=station_key,
        graph_type=graph_type,
        base_datetime=base_date,
        event_window_days=event_window_days,
    )
    return preview_input, threshold_file


def _build_sample_output_path(
    *,
    graph_type: str,
    event_window_days: int | None,
    station_key: str,
    base_datetime: str | None,
) -> Path:
    """連続実行で衝突しないサンプル出力パスを組み立てる。"""

    root = Path("outputs") / "hydrology_graphs" / "dev_preview_samples"
    date_part = datetime.now().strftime("%Y%m%d")
    run_part = datetime.now().strftime("%H%M%S_%f")
    graph_part = graph_type if event_window_days is None else f"{graph_type}_{event_window_days}day"
    base_part = base_datetime or "annual"
    stem = f"{run_part}_{station_key}_{graph_part}_{base_part}".replace(":", "-")
    candidate = root / date_part / f"{stem}.png"
    if not candidate.exists():
        return candidate
    index = 2
    while True:
        alt = root / date_part / f"{stem}_{index:02d}.png"
        if not alt.exists():
            return alt
        index += 1
