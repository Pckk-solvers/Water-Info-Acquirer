## 実装タスク（時間・日データ後処理）

### 0. 事前準備
- [ ] 対象Excelパスの決定（_H系/_D系）。試験用は `water_info/303031283303010_木原_2020年1月-2021年12月_WH.xlsx` / `_WD.xlsx`。
- [ ] 出力先ルートの決定（例: `out/`、Parquetは `parquet/`）。

### 1. 読み込み・正規化
- [ ] `load_hourly(path)` 実装: 先頭2列のみ読み込み（全期間>年シート）→列名を `display_dt`, `value` に正規化→`display_dt` を datetime64 へ→`hydro_date = (display_dt - 1h).dt.date` を追加。
- [ ] `load_daily(path)` 実装: 先頭2列のみ読み込み（全期間>年シート）→列名を `datetime`, `value` に正規化→`hydro_date = datetime.dt.date` を追加→`daily_value` にリネーム。

### 2. 時間→日集計
- [ ] `aggregate_hourly(df_hour_raw)` 実装:  
  - グループキー `hydro_date`。  
  - `count_non_null = value.count()`  
  - `hourly_daily_avg_var_den = value.mean(skipna=True)`  
  - `hourly_daily_avg_fixed_den = value.mean()` ただし `count_non_null == 24` 以外は NaN。  
  - 戻り: `hydro_date`, 上記3列。

### 3. マージ
- [ ] `merge_daily(df_hour_daily, df_daily_raw)` 実装: `hydro_date` で外部結合→`df_merged` にする。欠損はNaNのまま。

### 4. ランク付与（年単位）
- [ ] `add_ranks(df_merged)` 実装:  
  - `year = hydro_date.dt.year` を付与。  
  - 対象列: `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `daily_value`。  
  - 年・列ごとに欠損11個以上ならランク全NaN。  
  - それ以外: 非欠損を値降順でユニーク連番（最大=1、同値も連番）。欠損は日付昇順で続き番号を付与。  
  - 戻り: ランク列 `rank_var_den`, `rank_fixed_den`, `rank_daily_value` を追加したDF。

### 5. 位況算出（年単位、4種×3基準列）
- [ ] `add_ikyo(df_with_ranks, source_col)` を汎用化して、以下3基準それぞれで4種位況を算出:  
  - 基準列: `hourly_daily_avg_var_den` → 列名 `ikyo_high_var_den`, `ikyo_normal_var_den`, `ikyo_low_var_den`, `ikyo_drought_var_den`  
  - 基準列: `hourly_daily_avg_fixed_den` → 列名を `_fixed_den`  
  - 基準列: `daily_value` → 列名を `_daily_value`  
- [ ] 位況ロジック: 365日基準順位(豊95/平185/低275/渇355) → 365/366でスケール → 欠測10日以下なら `r_adj = floor(r * 有効日数 / 総日数)`、11日以上は算出しない → 降順ソートで順位の値を抽出 → 該当年の行に反映。

### 6. ピークサマリ作成
- [ ] `build_peaks(df_hour_raw)` 実装: 日別に最大/最小/ピーク時刻（例: 最大値とその時刻のみでOKか要確認。ここでは最大値・最大時刻を保持）を集計 → `df_summary_peak` へ。

### 7. 出力（Excel + Parquet、小数2位丸め）
- [ ] Excel: 複数シートで出力（暫定案: `main`=df_merged+ランク+位況, `peaks`=df_summary_peak, `year_summary` は空でも用意可）。  
- [ ] 丸め: 主要数値列を小数2位に丸めて出力。  
- [ ] Parquet: 個別DFを `parquet/df_hour_raw.parquet`, `parquet/df_hour_daily.parquet`, `parquet/df_merged.parquet`, `parquet/df_summary_peak.parquet` などで保存。

### 8. 簡易検証
- [ ] サンプルファイルで関数チェーンを実行し、位況・ランク・ピーク列が埋まることを確認。  
- [ ] 欠損が多い年（11個以上）でランク/位況がNaNになることを確認。  
- [ ] 固定分母平均が24本未満でNaNになることを確認。

### 未決・保留
- Excelシート名・順序は暫定（後で変更可）。  
- ピークサマリに最小値/本数などを含めるかは必要時に追加。  
- 特定列だけ異なる丸め桁が必要になったら追加指示で対応。
