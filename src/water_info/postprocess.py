"""Water Info post-processing utilities.

時間データ(_H)と日データ(_D)のExcelを読み込み、日次集計・ランク付与・位況算出・
ピークサマリを生成し、Excel/Parquetに出力する。
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Iterable

import pandas as pd
from decimal import Decimal, ROUND_HALF_UP


def load_hourly(path: str | Path) -> pd.DataFrame:
    """_H系Excelを読み込み、列名を正規化してhydro_dateを付与。"""
    file_path = Path(path)
    print(f"[INFO] 時間データ読込: {file_path}")
    xls = pd.ExcelFile(file_path)
    sheet = "全期間" if "全期間" in xls.sheet_names else None
    if sheet is None:
        candidates = [s for s in xls.sheet_names if s.endswith("年")]
        if not candidates:
            raise ValueError(f"シートが見つかりません: {file_path}")
        sheet = candidates
    df = pd.read_excel(file_path, sheet_name=sheet, usecols=[0, 1], header=0)
    df.columns = ["display_dt", "value"]
    df["display_dt"] = pd.to_datetime(df["display_dt"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce").apply(
        lambda x: float(Decimal(str(x)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
        if pd.notna(x)
        else math.nan
    )
    df["hydro_date"] = (df["display_dt"] - pd.Timedelta(hours=1)).dt.date
    return df


def load_daily(path: str | Path) -> pd.DataFrame:
    """_D系Excelを読み込み、列名を正規化してhydro_dateを付与。"""
    file_path = Path(path)
    print(f"[INFO] 日データ読込: {file_path}")
    xls = pd.ExcelFile(file_path)
    sheet = "全期間" if "全期間" in xls.sheet_names else None
    if sheet is None:
        candidates = [s for s in xls.sheet_names if s.endswith("年")]
        if not candidates:
            raise ValueError(f"シートが見つかりません: {file_path}")
        sheet = candidates
    df = pd.read_excel(file_path, sheet_name=sheet, usecols=[0, 1], header=0)
    df.columns = ["datetime", "daily_value"]
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["daily_value"] = pd.to_numeric(df["daily_value"], errors="coerce").apply(
        lambda x: float(Decimal(str(x)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
        if pd.notna(x)
        else math.nan
    )
    df["hydro_date"] = df["datetime"].dt.date
    return df


def aggregate_hourly(df_hour_raw: pd.DataFrame) -> pd.DataFrame:
    """時間データを1日（1:00~0:00）に集計。"""
    print(f"[INFO] 日次集計: 時間データ行数={len(df_hour_raw)}")
    grp = df_hour_raw.groupby("hydro_date", dropna=True)
    records = []
    for hydro_date, g in grp:
        # Decimalで合計・平均を計算して丸め誤差を抑制
        vals = [Decimal(str(v)) for v in g["value"].dropna()]
        count_non_null = len(vals)
        avg_var = _round_half_up_scalar(
            float((sum(vals) / count_non_null) if count_non_null else math.nan), ndigits=2
        )
        if count_non_null == 24:
            avg_fixed = _round_half_up_scalar(float(sum(vals) / 24), ndigits=2)
        else:
            avg_fixed = math.nan
        records.append(
            {
                "hydro_date": pd.to_datetime(hydro_date),
                "hourly_daily_avg_var_den": avg_var,
                "hourly_daily_avg_fixed_den": avg_fixed,
                "count_non_null": count_non_null,
            }
        )
    return pd.DataFrame(records)


def merge_daily(df_hour_daily: pd.DataFrame, df_daily_raw: pd.DataFrame) -> pd.DataFrame:
    """時間集計と日データをhydro_dateで外部結合。"""
    print(f"[INFO] マージ: 時間日次={len(df_hour_daily)}, 日データ={len(df_daily_raw)}")
    df_hour_daily = df_hour_daily.copy()
    df_daily_raw = df_daily_raw.copy()
    df_hour_daily["hydro_date"] = pd.to_datetime(df_hour_daily["hydro_date"])
    df_daily_raw["hydro_date"] = pd.to_datetime(df_daily_raw["hydro_date"])
    merged = pd.merge(df_hour_daily, df_daily_raw[["hydro_date", "daily_value"]], on="hydro_date", how="outer")
    merged["hydro_date"] = pd.to_datetime(merged["hydro_date"])
    merged.sort_values("hydro_date", inplace=True)
    # 読み込み時の揺れを避け、日データも2桁で確定
    merged["daily_value"] = _round_half_up_series(merged["daily_value"], ndigits=2)
    merged["year"] = merged["hydro_date"].dt.year
    return merged


def _rank_one_year(
    df_year: pd.DataFrame,
    col: str,
    apply_threshold: bool = True,
    rank_missing: bool = True,
    tie_breaker: str = "hydro_date",
) -> pd.Series:
    """1年分のデータにユニークランクを付与。

    apply_threshold=True のとき: 欠損11件以上なら全NaN。
    rank_missing=False のとき: 欠損行にはランクを付けない（NaNのまま）。
    """
    ser = _round_half_up_series(df_year[col], ndigits=2)
    is_na = ser.isna()
    if apply_threshold and is_na.sum() >= 11:
        return pd.Series([math.nan] * len(df_year), index=df_year.index, dtype="float64")

    df_tmp = df_year.copy()
    df_tmp["__rank_value"] = ser

    result = pd.Series(index=df_year.index, dtype="float64")
    non_null = df_tmp.loc[~is_na].sort_values(["__rank_value", tie_breaker], ascending=[False, True])
    ranks = range(1, len(non_null) + 1)
    result.loc[non_null.index] = list(ranks)

    if rank_missing:
        nulls = df_tmp.loc[is_na].sort_values(tie_breaker)
        start = len(non_null) + 1
        result.loc[nulls.index] = list(range(start, start + len(nulls)))
    return result


def add_ranks(df_merged: pd.DataFrame, target_cols: list[str] | None = None) -> pd.DataFrame:
    """ランク列を追加。欠損11件以上の年はNaN扱い。"""
    if target_cols is None:
        target_cols = [
            "hourly_daily_avg_var_den",
            "hourly_daily_avg_fixed_den",
            "daily_value",
        ]
    out = df_merged.copy()
    out["year"] = out["hydro_date"].dt.year
    print(f"[INFO] ランク付与: 行数={len(out)}, 年={sorted(out['year'].unique())}")
    for col in target_cols:
        rank_col = {
            "hourly_daily_avg_var_den": "rank_var_den",
            "hourly_daily_avg_fixed_den": "rank_fixed_den",
            "daily_value": "rank_daily_value",
        }.get(col)
        if rank_col is None or col not in out.columns:
            continue
        ranks = []
        for year, g in out.groupby("year", sort=True):
            ranks.append(_rank_one_year(g, col, apply_threshold=True, rank_missing=True, tie_breaker="hydro_date"))
        out[rank_col] = pd.concat(ranks).sort_index()
    return out


def add_ranks_no_threshold(df_merged: pd.DataFrame, target_cols: list[str] | None = None) -> pd.DataFrame:
    """ランク列を追加（欠損11件以上でも計算する参考版）。"""
    if target_cols is None:
        target_cols = [
            "hourly_daily_avg_var_den",
            "hourly_daily_avg_fixed_den",
            "daily_value",
        ]
    out = df_merged.copy()
    out["year"] = out["hydro_date"].dt.year
    print(f"[INFO] 参考ランク付与（閾値なし）: 行数={len(out)}, 年={sorted(out['year'].unique())}")
    for col in target_cols:
        rank_col = {
            "hourly_daily_avg_var_den": "rank_var_den",
            "hourly_daily_avg_fixed_den": "rank_fixed_den",
            "daily_value": "rank_daily_value",
        }.get(col)
        if rank_col is None or col not in out.columns:
            continue
        ranks = []
        for year, g in out.groupby("year", sort=True):
            # 可変/固定/日データを揃えるため、タイブレークは可変分母の値と同じ並びにする
            tie_key = "hourly_daily_avg_var_den"
            # 全列にタイブレーク列を持たせる
            g["_tie_key"] = _round_half_up_series(g[tie_key], ndigits=2)
            ranks.append(_rank_one_year(g, col, apply_threshold=False, rank_missing=True, tie_breaker="_tie_key"))
        out[rank_col] = pd.concat(ranks).sort_index()
    out.drop(columns=["_tie_key"], errors="ignore", inplace=True)
    return out


def _scaled_rank(base_rank: int, total_days: int, missing: int, apply_threshold: bool = True) -> int | None:
    """基準順位を日数・欠測に応じて補正。apply_threshold=False なら欠測閾値を無視。"""
    if apply_threshold and missing >= 11:
        return None
    if missing >= total_days:
        return None
    ratio = base_rank / 365
    r = math.floor(ratio * total_days)
    effective_days = total_days - missing
    r_adj = math.floor(r * effective_days / total_days)
    return max(1, r_adj)


def _calc_rank(base_rank: int, total_days: int, missing: int, apply_threshold: bool, use_scaling: bool) -> int | None:
    """位況で使う順位を計算。use_scaling=False なら補正なし、欠測閾値も無視可。"""
    if apply_threshold and missing >= 11:
        return None
    if missing >= total_days:
        return None
    if not use_scaling:
        return max(1, base_rank)
    return _scaled_rank(base_rank, total_days, missing, apply_threshold=apply_threshold)


def add_ikyo(
    df_with_ranks: pd.DataFrame,
    source_cols: Iterable[str],
    apply_threshold: bool = True,
    use_scaling: bool = True,
) -> pd.DataFrame:
    """位況4種を基準列ごとに追加。

    apply_threshold=False なら欠測閾値を無視した参考版。
    use_scaling=False なら基準順位を補正せずそのまま使用。
    """
    levels = [
        ("ikyo_high", 95),
        ("ikyo_normal", 185),
        ("ikyo_low", 275),
        ("ikyo_drought", 355),
    ]
    out = df_with_ranks.copy()
    out["year"] = out["hydro_date"].dt.year
    print(f"[INFO] 位況算出: 年={sorted(out['year'].unique())}")

    for src_col in source_cols:
        suffix = {
            "hourly_daily_avg_var_den": "var_den",
            "hourly_daily_avg_fixed_den": "fixed_den",
            "daily_value": "daily_value",
        }.get(src_col, src_col)

        for year, g in out.groupby("year", sort=True):
            ser = g[src_col]
            total_days = 366 if pd.Timestamp(year=year, month=1, day=1).is_leap_year else 365
            missing = ser.isna().sum()
            non_null = ser.dropna().sort_values(ascending=False)
            if non_null.empty:
                for name, _ in levels:
                    out.loc[g.index, f"{name}_{suffix}"] = math.nan
                continue

            for name, base_rank in levels:
                rk = _calc_rank(
                    base_rank=base_rank,
                    total_days=total_days,
                    missing=missing,
                    apply_threshold=apply_threshold,
                    use_scaling=use_scaling,
                )
                if rk is None:
                    out.loc[g.index, f"{name}_{suffix}"] = math.nan
                    continue
                if rk > len(non_null):
                    out.loc[g.index, f"{name}_{suffix}"] = math.nan
                    continue
                value = non_null.iloc[rk - 1]
                out.loc[g.index, f"{name}_{suffix}"] = value
    return out


def build_peaks(df_hour_raw: pd.DataFrame) -> pd.DataFrame:
    """日別の最大値とその時刻をまとめる。"""
    print(f"[INFO] ピーク計算: 時間データ行数={len(df_hour_raw)}")
    records = []
    for hydro_date, g in df_hour_raw.groupby("hydro_date", dropna=True):
        non_null = g.dropna(subset=["value"])
        if non_null.empty:
            records.append({"hydro_date": hydro_date, "peak_max_value": math.nan, "peak_max_time": pd.NaT})
            continue
        idx = non_null["value"].idxmax()
        rec = non_null.loc[idx]
        records.append(
            {
                "hydro_date": hydro_date,
                "peak_max_value": rec["value"],
                "peak_max_time": rec["display_dt"],
            }
        )
    df = pd.DataFrame(records)
    df["hydro_date"] = pd.to_datetime(df["hydro_date"])
    df.sort_values("hydro_date", inplace=True)
    return df


def _round_half_up_scalar(val: float, ndigits: int = 2) -> float:
    """単一の数値を四捨五入（ROUND_HALF_UP）。NaNはそのまま。"""
    if pd.isna(val):
        return math.nan
    q = Decimal("1").scaleb(-ndigits)  # 10**-ndigits
    return float(Decimal(str(val)).quantize(q, rounding=ROUND_HALF_UP))


def _round_half_up_series(series: pd.Series, ndigits: int = 2) -> pd.Series:
    return series.apply(lambda x: _round_half_up_scalar(x, ndigits=ndigits))


def _round_numeric(df: pd.DataFrame, ndigits: int = 2) -> pd.DataFrame:
    out = df.copy()
    num_cols = out.select_dtypes(include="number").columns
    for col in num_cols:
        out[col] = _round_half_up_series(out[col], ndigits=ndigits)
    return out



def _rename_for_excel(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Excel出力時に列名を日本語へ寄せる。"""
    renamed = df.copy()
    cols = {c: mapping[c] for c in df.columns if c in mapping}
    renamed = renamed.rename(columns=cols)
    return renamed


def build_year_summary(
    df_with_ikyo: pd.DataFrame,
    df_hour_raw: pd.DataFrame,
    source_cols: Iterable[str],
    apply_threshold: bool = True,
    use_scaling: bool = True,
) -> pd.DataFrame:
    """年単位の欠損数・平均・時間値の最大/最小・位況をまとめる。"""
    records = []
    target_cols = list(source_cols)
    ikyo_levels = [
        "ikyo_high",
        "ikyo_normal",
        "ikyo_low",
        "ikyo_drought",
    ]
    suffix_map = {
        "hourly_daily_avg_var_den": "var_den",
        "hourly_daily_avg_fixed_den": "fixed_den",
        "daily_value": "daily_value",
    }
    # 利用する列だけに絞る
    suffix_map = {k: v for k, v in suffix_map.items() if k in target_cols}
    base_ranks = [
        ("ikyo_high", 95),
        ("ikyo_normal", 185),
        ("ikyo_low", 275),
        ("ikyo_drought", 355),
    ]

    for year, g in df_with_ikyo.groupby(df_with_ikyo["hydro_date"].dt.year, sort=True):
        rec: dict[str, object] = {"year": year}
        total_days = 366 if pd.Timestamp(year=year, month=1, day=1).is_leap_year else 365
        for col in target_cols:
            suffix = suffix_map[col]
            ser = g[col]
            rec[f"missing_{suffix}"] = ser.isna().sum()
            vals = [Decimal(str(v)) for v in ser.dropna()]
            if vals:
                mean_val = float((sum(vals) / len(vals)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            else:
                mean_val = math.nan
            rec[f"mean_{suffix}"] = mean_val
            for lvl in ikyo_levels:
                col_name = f"{lvl}_{suffix_map[col]}"
                ser_ikyo = g[col_name] if col_name in g.columns else pd.Series(dtype=float)
                val = ser_ikyo.dropna().iloc[0] if not ser_ikyo.dropna().empty else math.nan
                rec[col_name] = val
        # 時間データから最大/最小とその時刻（display_dt）を取得
        g_hour = df_hour_raw[df_hour_raw["display_dt"].dt.year == year]
        if g_hour.dropna(subset=["value"]).empty:
            rec["max_hourly_value"] = math.nan
            rec["max_hourly_time"] = pd.NaT
            rec["min_hourly_value"] = math.nan
            rec["min_hourly_time"] = pd.NaT
        else:
            idx_max = g_hour["value"].idxmax()
            rec["max_hourly_value"] = _round_half_up_scalar(g_hour.loc[idx_max, "value"], ndigits=2)
            rec["max_hourly_time"] = g_hour.loc[idx_max, "display_dt"]
            idx_min = g_hour["value"].idxmin()
            rec["min_hourly_value"] = _round_half_up_scalar(g_hour.loc[idx_min, "value"], ndigits=2)
            rec["min_hourly_time"] = g_hour.loc[idx_min, "display_dt"]
        # 位況で採用した順位を記録
        for col in target_cols:
            suffix = suffix_map[col]
            ser = g[col]
            missing = ser.isna().sum()
            for lvl_name, base_rank in base_ranks:
                rk = _calc_rank(
                    base_rank=base_rank,
                    total_days=total_days,
                    missing=missing,
                    apply_threshold=apply_threshold,
                    use_scaling=use_scaling,
                )
                rec[f"rank_used_{lvl_name}_{suffix}"] = rk
        records.append(rec)
    return pd.DataFrame(records)


def export_excel(dfs: dict[str, pd.DataFrame], path: str | Path) -> None:
    """複数DFをExcelにシート分けで出力。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Excel出力: {path}")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet, df in dfs.items():
            rounded = _round_numeric(df, ndigits=2)
            rounded.to_excel(writer, sheet_name=sheet, index=False)


def export_parquet(dfs: dict[str, pd.DataFrame], root: str | Path) -> None:
    """各DFを個別のParquetに保存。"""
    if root is None:
        return
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        print(f"[INFO] Parquet出力: {root/name}.parquet")
        df.to_parquet(root / f"{name}.parquet")


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Water Info post-processing")
    p.add_argument("--hour-file", required=True, help="_H系Excelファイルへのパス")
    p.add_argument("--daily-file", required=False, help="_D系Excelファイルへのパス")
    p.add_argument("--out-excel", required=True, help="出力Excelパス")
    p.add_argument("--out-parquet", required=False, help="Parquet出力ディレクトリ（未指定なら出力しない）")
    p.add_argument("--sheet-main", default="main", help="メインシート名")
    p.add_argument("--sheet-main-raw", default="main_raw_rank", help="参考ランク版メインシート名（閾値なし）")
    p.add_argument("--sheet-peaks", default="peaks", help="ピークシート名")
    p.add_argument("--sheet-year-summary", default="year_summary", help="年サマリシート名（未使用なら空のまま）")
    p.add_argument("--sheet-year-summary-raw", default="year_summary_raw", help="参考ランク版年サマリシート名")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    df_hour_raw = load_hourly(args.hour_file)
    source_cols_base = [
        "hourly_daily_avg_var_den",
        "hourly_daily_avg_fixed_den",
        "daily_value",
    ]
    if args.daily_file:
        df_daily_raw = load_daily(args.daily_file)
        df_hour_daily = aggregate_hourly(df_hour_raw)
        df_merged = merge_daily(df_hour_daily, df_daily_raw)
        source_cols = source_cols_base
        df_ranked = add_ranks(df_merged, target_cols=source_cols)
        df_ranked_raw = add_ranks_no_threshold(df_merged, target_cols=source_cols)
        df_ikyo = add_ikyo(df_ranked, source_cols, apply_threshold=True, use_scaling=True)
        df_ikyo_raw = add_ikyo(df_ranked_raw, source_cols, apply_threshold=False, use_scaling=False)
        df_peaks = build_peaks(df_hour_raw)
        df_year_summary = build_year_summary(df_ikyo, df_hour_raw, source_cols, apply_threshold=True, use_scaling=True)
        df_year_summary_raw = build_year_summary(
            df_ikyo_raw, df_hour_raw, source_cols, apply_threshold=False, use_scaling=False
        )

        # Excel用の列名マッピング
        main_map = {
            "hydro_date": "日付",
            "hourly_daily_avg_var_den": "日平均（可変分母）",
            "hourly_daily_avg_fixed_den": "日平均（固定分母）",
            "daily_value": "日データ",
            "rank_var_den": "ランク（可変分母）",
            "rank_fixed_den": "ランク（固定分母）",
            "rank_daily_value": "ランク（日データ）",
            "count_non_null": "非欠損本数",
            # 位況（可変分母）
            "ikyo_high_var_den": "位況（豊水位,可変分母）",
            "ikyo_normal_var_den": "位況（平水位,可変分母）",
            "ikyo_low_var_den": "位況（低水位,可変分母）",
            "ikyo_drought_var_den": "位況（渇水位,可変分母）",
            # 位況（固定分母）
            "ikyo_high_fixed_den": "位況（豊水位,固定分母）",
            "ikyo_normal_fixed_den": "位況（平水位,固定分母）",
            "ikyo_low_fixed_den": "位況（低水位,固定分母）",
            "ikyo_drought_fixed_den": "位況（渇水位,固定分母）",
            # 位況（日データ）
            "ikyo_high_daily_value": "位況（豊水位,日データ）",
            "ikyo_normal_daily_value": "位況（平水位,日データ）",
            "ikyo_low_daily_value": "位況（低水位,日データ）",
            "ikyo_drought_daily_value": "位況（渇水位,日データ）",
            "year": "年",
        }
        peak_map = {
            "hydro_date": "日付",
            "peak_max_value": "最高値",
            "peak_max_time": "最高時刻（水水DB基準）",
        }
        year_map = {
            "year": "年",
            "missing_var_den": "欠損数（可変分母）",
            "missing_fixed_den": "欠損数（固定分母）",
            "missing_daily_value": "欠損数（日データ）",
            "mean_var_den": "平均（可変分母）",
            "mean_fixed_den": "平均（固定分母）",
            "mean_daily_value": "平均（日データ）",
            "max_hourly_value": "最大（1時間値）",
            "max_hourly_time": "最大生起日時（水水DB基準）",
            "min_hourly_value": "最小（1時間値）",
            "min_hourly_time": "最小生起日時（水水DB基準）",
            "rank_used_ikyo_high_var_den": "採用順位（豊水位,可変分母）",
            "rank_used_ikyo_normal_var_den": "採用順位（平水位,可変分母）",
            "rank_used_ikyo_low_var_den": "採用順位（低水位,可変分母）",
            "rank_used_ikyo_drought_var_den": "採用順位（渇水位,可変分母）",
            "rank_used_ikyo_high_fixed_den": "採用順位（豊水位,固定分母）",
            "rank_used_ikyo_normal_fixed_den": "採用順位（平水位,固定分母）",
            "rank_used_ikyo_low_fixed_den": "採用順位（低水位,固定分母）",
            "rank_used_ikyo_drought_fixed_den": "採用順位（渇水位,固定分母）",
            "rank_used_ikyo_high_daily_value": "採用順位（豊水位,日データ）",
            "rank_used_ikyo_normal_daily_value": "採用順位（平水位,日データ）",
            "rank_used_ikyo_low_daily_value": "採用順位（低水位,日データ）",
            "rank_used_ikyo_drought_daily_value": "採用順位（渇水位,日データ）",
            "ikyo_high_var_den": "位況豊水位（可変分母）",
            "ikyo_normal_var_den": "位況平水位（可変分母）",
            "ikyo_low_var_den": "位況低水位（可変分母）",
            "ikyo_drought_var_den": "位況渇水位（可変分母）",
            "ikyo_high_fixed_den": "位況豊水位（固定分母）",
            "ikyo_normal_fixed_den": "位況平水位（固定分母）",
            "ikyo_low_fixed_den": "位況低水位（固定分母）",
            "ikyo_drought_fixed_den": "位況渇水位（固定分母）",
            "ikyo_high_daily_value": "位況豊水位（日データ）",
            "ikyo_normal_daily_value": "位況平水位（日データ）",
            "ikyo_low_daily_value": "位況低水位（日データ）",
            "ikyo_drought_daily_value": "位況渇水位（日データ）",
        }

        df_main_excel = _rename_for_excel(df_ikyo, main_map)
        df_main_raw_excel = _rename_for_excel(df_ikyo_raw, main_map)
        df_peaks_excel = _rename_for_excel(df_peaks, peak_map)
        df_year_excel = _rename_for_excel(df_year_summary, year_map).T.reset_index()
        df_year_excel.columns = ["項目"] + df_year_excel.columns[1:].tolist()
        df_year_raw_excel = _rename_for_excel(df_year_summary_raw, year_map).T.reset_index()
        df_year_raw_excel.columns = ["項目"] + df_year_raw_excel.columns[1:].tolist()

        export_excel(
            {
                args.sheet_main: df_main_excel,
                args.sheet_main_raw: df_main_raw_excel,
                args.sheet_peaks: df_peaks_excel,
                args.sheet_year_summary: df_year_excel,
                args.sheet_year_summary_raw: df_year_raw_excel,
            },
            args.out_excel,
        )
        export_parquet(
            {
                "df_hour_raw": df_hour_raw,
                "df_hour_daily": df_hour_daily,
                "df_merged": df_ikyo,
                "df_summary_peak": df_peaks,
            },
            args.out_parquet,
        )
    else:
        # 日データが無い場合: 時間集計とピークのみ出力（main/main_raw/year_summary も空でなく、ある列だけ出す）
        print("[INFO] 日データ未指定のため時間データのみ出力します")
        df_hour_daily = aggregate_hourly(df_hour_raw)
        df_peaks = build_peaks(df_hour_raw)
        # main/main_raw は時間集計のみ
        df_main = df_hour_daily.copy()
        df_main["year"] = pd.to_datetime(df_main["hydro_date"]).dt.year
        source_cols = ["hourly_daily_avg_var_den", "hourly_daily_avg_fixed_den"]
        df_ranked = add_ranks(df_main, target_cols=source_cols)
        df_ranked_raw = add_ranks_no_threshold(df_main, target_cols=source_cols)
        df_ikyo = add_ikyo(df_ranked, source_cols=source_cols, apply_threshold=True, use_scaling=True)
        df_ikyo_raw = add_ikyo(
            df_ranked_raw, source_cols=source_cols, apply_threshold=False, use_scaling=False
        )
        df_year_summary = build_year_summary(
            df_ikyo,
            df_hour_raw,
            source_cols=source_cols,
            apply_threshold=True,
            use_scaling=True,
        )
        df_year_summary_raw = build_year_summary(
            df_ikyo_raw,
            df_hour_raw,
            source_cols=source_cols,
            apply_threshold=False,
            use_scaling=False,
        )
        # マッピング（daily関連を除外）
        main_map = {
            "hydro_date": "日付",
            "hourly_daily_avg_var_den": "日平均（可変分母）",
            "hourly_daily_avg_fixed_den": "日平均（固定分母）",
            "count_non_null": "非欠損本数",
            "year": "年",
            "rank_var_den": "ランク（可変分母）",
            "rank_fixed_den": "ランク（固定分母）",
            "ikyo_high_var_den": "位況豊水位（可変分母）",
            "ikyo_normal_var_den": "位況平水位（可変分母）",
            "ikyo_low_var_den": "位況低水位（可変分母）",
            "ikyo_drought_var_den": "位況渇水位（可変分母）",
            "ikyo_high_fixed_den": "位況豊水位（固定分母）",
            "ikyo_normal_fixed_den": "位況平水位（固定分母）",
            "ikyo_low_fixed_den": "位況低水位（固定分母）",
            "ikyo_drought_fixed_den": "位況渇水位（固定分母）",
        }
        peak_map = {
            "hydro_date": "日付",
            "peak_max_value": "最高値",
            "peak_max_time": "最高時刻（水水DB基準）",
        }
        year_map = {
            "year": "年",
            "missing_var_den": "欠損数（可変分母）",
            "missing_fixed_den": "欠損数（固定分母）",
            "mean_var_den": "平均（可変分母）",
            "mean_fixed_den": "平均（固定分母）",
            "max_hourly_value": "最大（1時間値）",
            "max_hourly_time": "最大生起日時（水水DB基準）",
            "min_hourly_value": "最小（1時間値）",
            "min_hourly_time": "最小生起日時（水水DB基準）",
            "rank_used_ikyo_high_var_den": "採用順位（豊水位,可変分母）",
            "rank_used_ikyo_normal_var_den": "採用順位（平水位,可変分母）",
            "rank_used_ikyo_low_var_den": "採用順位（低水位,可変分母）",
            "rank_used_ikyo_drought_var_den": "採用順位（渇水位,可変分母）",
            "rank_used_ikyo_high_fixed_den": "採用順位（豊水位,固定分母）",
            "rank_used_ikyo_normal_fixed_den": "採用順位（平水位,固定分母）",
            "rank_used_ikyo_low_fixed_den": "採用順位（低水位,固定分母）",
            "rank_used_ikyo_drought_fixed_den": "採用順位（渇水位,固定分母）",
            "ikyo_high_var_den": "位況豊水位（可変分母）",
            "ikyo_normal_var_den": "位況平水位（可変分母）",
            "ikyo_low_var_den": "位況低水位（可変分母）",
            "ikyo_drought_var_den": "位況渇水位（可変分母）",
            "ikyo_high_fixed_den": "位況豊水位（固定分母）",
            "ikyo_normal_fixed_den": "位況平水位（固定分母）",
            "ikyo_low_fixed_den": "位況低水位（固定分母）",
            "ikyo_drought_fixed_den": "位況渇水位（固定分母）",
        }
        df_main_excel = _rename_for_excel(df_ikyo, main_map)
        df_main_raw_excel = _rename_for_excel(df_ikyo_raw, main_map)
        df_peaks_excel = _rename_for_excel(df_peaks, peak_map)
        df_year_excel = _rename_for_excel(df_year_summary, year_map).T.reset_index()
        df_year_excel.columns = ["項目"] + df_year_excel.columns[1:].tolist()
        df_year_raw_excel = _rename_for_excel(df_year_summary_raw, year_map).T.reset_index()
        df_year_raw_excel.columns = ["項目"] + df_year_raw_excel.columns[1:].tolist()
        export_excel(
            {
                args.sheet_main: _round_numeric(df_main_excel, ndigits=2),
                args.sheet_main_raw: _round_numeric(df_main_raw_excel, ndigits=2),
                args.sheet_peaks: _round_numeric(df_peaks_excel, ndigits=2),
                args.sheet_year_summary: _round_numeric(df_year_excel, ndigits=2),
                args.sheet_year_summary_raw: _round_numeric(df_year_raw_excel, ndigits=2),
            },
            args.out_excel,
        )
        export_parquet(
            {
                "df_hour_raw": df_hour_raw,
                "df_hour_daily": df_hour_daily,
                "df_summary_peak": df_peaks,
            },
            args.out_parquet,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
