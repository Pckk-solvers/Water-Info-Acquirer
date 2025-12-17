## Implementation Plan (Python Post-Processing)

### 1. ファイル配置・役割
- 追加スクリプト: `src/water_info/postprocess.py`（water_info配下に配置）。  
- 目的: 時間データと日データのExcelを読み込み、日次集計・ランク・位況・ピークサマリを生成し、Excel/Parquetに出力する。

### 2. 入出力と列スキーマ
- 入力ファイル: _H系Excel（例: WH/QH/RH）、_D系Excel（例: WD/QD/RD）。  
- 読み込み後の列名（正規化）:
  - 時間: `display_dt`, `value`, `hydro_date` (=(display_dt - 1h).date)
  - 日: `datetime`, `daily_value`, `hydro_date` (=datetime.date)
- 集計結果（df_hour_daily）:
  - `hydro_date`, `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `count_non_null`
- マージ結果（df_merged）:
  - `hydro_date`, 上記2平均列, `daily_value`, ランク列, 位況列, `year`
- 位況列（3基準×4種）:
  - 例: `ikyo_high_var_den`, `ikyo_normal_var_den`, `ikyo_low_var_den`, `ikyo_drought_var_den`
  - 同様に `_fixed_den`, `_daily_value`
- ピークサマリ（df_summary_peak）:
  - `hydro_date`, `peak_max_value`, `peak_max_time`（必要ならminも追加）

### 3. 関数IF（関数名は調整可）
1) `load_hourly(path: str) -> pd.DataFrame`  
2) `load_daily(path: str) -> pd.DataFrame`  
3) `aggregate_hourly(df_hour_raw: pd.DataFrame) -> pd.DataFrame`  
4) `merge_daily(df_hour_daily: pd.DataFrame, df_daily_raw: pd.DataFrame) -> pd.DataFrame`  
5) `add_ranks(df_merged: pd.DataFrame) -> pd.DataFrame`  
6) `add_ikyo(df_with_ranks: pd.DataFrame, source_col: str) -> pd.DataFrame`  
   - 呼び出し側で3基準列を順番に適用し、位況4種を列追加。  
7) `build_peaks(df_hour_raw: pd.DataFrame) -> pd.DataFrame`  
8) `export_excel(dfs: dict[str, pd.DataFrame], path: str)`  
   - 暫定シート名: `main`, `peaks`, `year_summary`（後で変更可）  
9) `export_parquet(dfs: dict[str, pd.DataFrame], root: str)`  
   - 例: `parquet/df_hour_raw.parquet` 等
10) `main()`/CLI: 入力パス/出力先を受け取り、上記関数を順次実行。

### 4. 主要ロジックの要点
- 読み込み:
  - `"全期間"`シートがあれば優先、無ければ年シート（^\d{4}年$）を全読み込みしてconcat。  
  - `usecols=[0,1]`, `header=0`。列名リネーム後に型変換（to_datetime, astype(float)）。
- 時間→日集計:
  - グループキー `hydro_date`  
  - `count_non_null = value.count()`  
  - 可変分母平均 = `value.mean(skipna=True)`  
  - 分母固定平均 = `value.mean()` ただし `count_non_null==24` 以外はNaN
- マージ:
  - `hydro_date`で外部結合。欠損はNaNのまま。
- ランク:
  - 対象列: `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `daily_value`  
  - 年単位で欠損11個以上なら当該列ランク全NaN。  
  - 非欠損: 値降順ユニーク連番（最大=1）。同値も連番。  
  - 欠損: 日付昇順で続き番号を付与。  
- 位況:
  - 基準順位（365日ベース）: 豊=95, 平=185, 低=275, 渇=355。  
  - 365/366日でスケール→欠測10日以下なら `r_adj = floor(r * 有効日数 / 総日数)`、11日以上は算出なし。  
  - 降順ソートで調整後順位の値を拾い、基準列ごとに4列を追加。  
- ピーク:
  - `hydro_date`単位で最大値とその時刻を算出し、`df_summary_peak`に保持。
- 丸め:
  - 出力前に主要数値列を小数2位で丸める（round(2)）。

### 5. 出力仕様
- Excel: 複数シート（暫定: main/peaks/year_summary）。  
- Parquet: 個別DFごとに `parquet/` 以下へ保存。  
- 列名: 英字で統一。必要なら値ラベル（例: 水位/流量/雨量）はメタやファイル名で管理。

### 6. 簡易CLI想定
```
uv run python src/water_info/postprocess.py ^
  --hour-file water_info/..._WH.xlsx ^
  --daily-file water_info/..._WD.xlsx ^
  --out-excel out/result.xlsx ^
  --out-parquet parquet
```
オプション例: `--sheet-main`, `--sheet-peaks`, `--sheet-year-summary` でシート名を上書き可とする。

### 7. 残タスク/確認事項
- Excelシート名の最終決定は後で可（暫定のまま実装し、後で変更）。  
- ピークサマリに最小値や本数を含めるかは必要時に拡張。  
- 追加の丸め設定（列ごとに桁数を変える必要があれば後から対応）。
