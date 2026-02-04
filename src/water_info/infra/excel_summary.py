"""Summary builders for water_info Excel outputs."""

from __future__ import annotations


def build_daily_empty_summary(pd, df, value_col: str):
    tmp = pd.DataFrame(
        [[dt.strftime('%Y/%m/%d'), val] for dt, val in zip(df['display_dt'], df[value_col])],
        columns=['date', value_col],
    )
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors='coerce')
    daily_df = (
        tmp
        .groupby('date')
        .agg(empty_count=(value_col, lambda s: s.isna().sum()))
        .reset_index()
    )
    return daily_df


def build_year_summary(pd, df, value_col: str):
    year_list = []
    for year, group in df.groupby('sheet_year', sort=True):
        non_null = group[value_col].dropna()
        if non_null.empty:
            continue
        max_idx = non_null.idxmax()
        ts_max = group.loc[max_idx, 'display_dt'].to_pydatetime()
        val_max = group.loc[max_idx, value_col]
        empty_year = group[value_col].isna().sum()
        year_list.append([year, ts_max, val_max, empty_year])

    return pd.DataFrame(
        year_list,
        columns=['year', 'year_max_datetime', value_col, 'year_empty_count'],
    )


def build_sheet_stats(grp_df, value_col: str):
    vals = grp_df[value_col]
    valid = vals.dropna()
    if not valid.empty:
        sheet_max_val = valid.max()
        sheet_max_date = grp_df.loc[valid.idxmax(), "datetime"].strftime("%Y/%m/%d")
        sheet_min_val = valid.min()
        sheet_min_date = grp_df.loc[valid.idxmin(), "datetime"].strftime("%Y/%m/%d")
        sheet_avg_val = valid.mean()
    else:
        sheet_max_date = sheet_min_date = sheet_avg_val = ""
        sheet_max_val = sheet_min_val = ""
    sheet_empty = int(vals.isna().sum())
    return {
        "sheet_max_date": sheet_max_date,
        "sheet_max_val": sheet_max_val,
        "sheet_min_date": sheet_min_date,
        "sheet_min_val": sheet_min_val,
        "sheet_avg_val": sheet_avg_val,
        "sheet_empty": sheet_empty,
    }
