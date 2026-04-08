from __future__ import annotations

import pandas as pd
from tkinter import messagebox


def recompute_base_date_candidates_for_selected_stations(app) -> None:
    """選択条件に合う基準日候補を更新する。"""

    catalog = app._catalog
    if catalog is None or catalog.data.empty:
        app._base_dates = []
    else:
        selected_pairs = app._selected_station_pairs_in_order()
        if not selected_pairs:
            app._base_dates = []
        else:
            station_frame = catalog.data.loc[:, ["source", "station_key", "observed_at"]].copy()
            selected_frame = pd.DataFrame(
                {
                    "source": [source for source, _ in selected_pairs],
                    "station_key": [station_key for _, station_key in selected_pairs],
                }
            )
            station_frame = station_frame.merge(selected_frame, on=["source", "station_key"], how="inner")
            observed = pd.to_datetime(station_frame["observed_at"], errors="coerce").dropna()
            app._base_dates = sorted({ts.date().isoformat() for ts in observed})
    app._refresh_base_date_ymd_controls()


def refresh_base_date_ymd_controls(app) -> None:
    """基準日候補を YYYY/MM/DD の3プルダウンへ反映する。"""

    year_to_months: dict[str, set[str]] = {}
    year_month_to_days: dict[tuple[str, str], set[str]] = {}
    for iso_date in app._base_dates:
        if len(iso_date) < 10:
            continue
        year = iso_date[0:4]
        month = iso_date[5:7]
        day = iso_date[8:10]
        year_to_months.setdefault(year, set()).add(month)
        year_month_to_days.setdefault((year, month), set()).add(day)

    app._base_date_year_to_months = {k: sorted(v) for k, v in year_to_months.items()}
    app._base_date_year_month_to_days = {k: sorted(v) for k, v in year_month_to_days.items()}

    years = sorted(app._base_date_year_to_months.keys())
    year_combo = getattr(app, "base_date_year_combo", None)
    if year_combo is not None:
        year_combo.configure(values=years)
    selected_year = app.base_date_year.get().strip()
    if selected_year not in years:
        selected_year = years[0] if years else ""
        app.base_date_year.set(selected_year)

    months = app._base_date_year_to_months.get(selected_year, [])
    month_combo = getattr(app, "base_date_month_combo", None)
    if month_combo is not None:
        month_combo.configure(values=months)
    selected_month = app.base_date_month.get().strip()
    if selected_month not in months:
        selected_month = months[0] if months else ""
        app.base_date_month.set(selected_month)

    days = app._base_date_year_month_to_days.get((selected_year, selected_month), [])
    candidate_combo = getattr(app, "base_date_candidate_combo", None)
    if candidate_combo is not None:
        candidate_combo.configure(values=days)
    selected_day = app.base_date_candidate.get().strip()
    if selected_day not in days:
        selected_day = days[0] if days else ""
        app.base_date_candidate.set(selected_day)

    app.base_date_list.delete(0, "end")
    for base_date in app.selected_base_dates:
        app.base_date_list.insert("end", base_date)


def on_base_date_year_changed(app, _event=None) -> None:
    year = app.base_date_year.get().strip()
    months = app._base_date_year_to_months.get(year, [])
    app.base_date_month_combo.configure(values=months)
    if app.base_date_month.get().strip() not in months:
        app.base_date_month.set(months[0] if months else "")
    on_base_date_month_changed(app)


def on_base_date_month_changed(app, _event=None) -> None:
    year = app.base_date_year.get().strip()
    month = app.base_date_month.get().strip()
    days = app._base_date_year_month_to_days.get((year, month), [])
    app.base_date_candidate_combo.configure(values=days)
    if app.base_date_candidate.get().strip() not in days:
        app.base_date_candidate.set(days[0] if days else "")


def current_base_date_candidate_iso(app) -> str | None:
    year = app.base_date_year.get().strip()
    month = app.base_date_month.get().strip()
    day = app.base_date_candidate.get().strip()
    if len(year) != 4 or len(month) != 2 or len(day) != 2:
        return None
    if (year not in app._base_date_year_to_months) or (month not in app._base_date_year_to_months.get(year, [])):
        return None
    if day not in app._base_date_year_month_to_days.get((year, month), []):
        return None
    return f"{year}-{month}-{day}"


def ensure_full_catalog_loaded(app) -> bool:
    """詳細読込が必要な処理向けにフルカタログを確保する。"""

    if app._catalog is not None and not app._catalog.data.empty:
        return True
    parquet_dir = app.parquet_dir.get().strip()
    if not parquet_dir:
        messagebox.showerror("入力エラー", "Parquet ディレクトリを指定してください。")
        return False
    try:
        app._append_log(f"[SCAN] detailed load start {parquet_dir}")
        catalog = app.service.scan_catalog(parquet_dir)
    except Exception as exc:  # noqa: BLE001
        messagebox.showerror("読込エラー", str(exc))
        return False
    app._catalog = catalog
    app._append_log(
        f"[SCAN] detailed load done rows={len(catalog.data)} invalid_files={len(catalog.invalid_files)}"
    )
    return True
