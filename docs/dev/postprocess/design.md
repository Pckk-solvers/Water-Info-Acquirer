## 設計方針（集計・マージ・拡張）

### 目的
国交省・水文データ（時間データと日データ）の後処理を行い、日次集計、ランク付与、位況算出、日・時間のリンクを拡張しやすい形で実装可能な粒度に落とす。

### データフレーム構成（案A: 用途別テーブル分割）
1. `df_hour_raw`  
   - 入力: _H系Excel（WH/QH/RH…）の「全期間」または年シートから先頭2列のみ読み込み。  
   - 列: `display_dt` (datetime64[ns]), `value` (float). 読み込み後すぐに列名正規化。
   - 備考: summaryシートは無視。  

2. `df_daily_raw`  
   - 入力: _D系Excel（WD/QD/RD）の「全期間」または年シートから先頭2列のみ読み込み。  
   - 列: `datetime` (datetime64[ns] or date), `value` (float). 読み込み後に列名正規化。

3. `df_hour_daily`（時間→日集計結果）  
   - 作成: `df_hour_raw` から以下を生成。  
     - `hydro_date = (display_dt - 1h).dt.date`（1:00~0:00を同一日扱い）。  
     - グループキー: `hydro_date`。  
     - 集計列:  
       - `hourly_daily_avg_var_den`: 可変分母平均（非NaNで割る）。  
       - `hourly_daily_avg_fixed_den`: 24本揃った日の平均。24未満ならNaN。  
       - `count_non_null`（任意; 分母判定用）。  

4. `df_merged`（主テーブル）  
   - 作成: `df_hour_daily` と `df_daily_raw` を `hydro_date` で外部結合。  
   - 列例:  
     - `hydro_date`  
     - `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`  
     - `daily_value`（日データの列名を正規化してマージ）  
     - ランク列: `rank_var_den`, `rank_fixed_den`, `rank_daily_value`  
     - 位況列（年ごとに値を反映）: 4種すべて出力（`ikyo_high`, `ikyo_normal`, `ikyo_low`, `ikyo_drought`）
   - 役割: 出力・下流サマリの基盤。

5. 拡張用サマリ（必要に応じ追加）  
   - 例: `df_summary_year`（年別欠測数・有効日数・位況値）、`df_summary_peak`（ランク上位日の時間ピーク時刻・値）。  
   - 用途別にDFを増やす方針で、主テーブルに無理に詰めない。

### 読み込みと列正規化
- Excel読み込み: 先頭2列のみ（A=日時, B=値）。`usecols=[0,1]`, `header=0`。  
- シート選択: `"全期間"`があれば優先、無ければ `^\d{4}年$` を全て読み込みconcat。  
- 列名統一: 時間データは `display_dt`, `value` にリネーム。日データは `datetime`, `value` にリネーム。  
- 日データの日付化: `hydro_date = datetime.dt.date` に変換し、マージ用キーにリネーム。  
- 時間データの日付化: `hydro_date = (display_dt - 1h).dt.date` を追加。

### 日次集計（時間データ -> 日平均）
- グループキー: `hydro_date`。  
- 集計処理:  
  - 可変分母平均 = `group.value.mean(skipna=True)`  
  - 分母固定平均 = `group.value.mean()` ただし `count_non_null == 24` の日のみ、足りなければNaN。  
  - 非NaN件数 `count_non_null = group.value.count()` を保持。  

### マージ
- `df_hour_daily` と `df_daily_raw`（列名を `daily_value` にリネーム）を `hydro_date` で外部結合し、`df_merged` を得る。  
- 欠損はNaNのまま保持。

### ランク付与（年単位）
- 対象列: `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `daily_value`。  
- 年日数は365/366を意識（ただし順位スケーリングではなく欠測閾値や分母固定の24本判定で利用）。  
- 手順:  
  1) `df_merged` に `year = hydro_date.dt.year` を付与。  
  2) 年・列ごとに処理。欠損( NaN )が11個以上ならその年の当該列ランクを全てNaN。  
  3) 欠損が閾値未満なら、非欠損を値降順でソートしユニーク連番（最大=1）。同値も連番。  
  4) 欠損にもランクを付与。日付昇順で非欠損の次の番号から連番。  
  5) 結果を `rank_var_den`, `rank_fixed_den`, `rank_daily_value` に格納。

### 位況算出（年単位）
- 基準順位（365日ベース資料値）: 豊=95, 平=185, 低=275, 渇=355。  
- うるう年補正: 比率 = 95/365 等を対象年日数(365/366)に掛け、`floor`。  
- 欠測補正: 欠測0日ならそのまま。欠測10日以下なら `r_adj = floor(r * 有効日数 / 総日数)`。欠測11日以上なら位況は算出しない。  
- 抽出: ランク対象の3列（`hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `daily_value`）それぞれを基準に、降順ソート→調整後順位で値を4種すべて（豊/平/低/渇）に反映し、列名で基準を区別する（例: `ikyo_high_var_den`, `ikyo_high_fixed_den`, `ikyo_high_daily_value`, ...）。

### リンク運用（時間ピークと日次指標）
- キー: `hydro_date`。  
- ランク/位況で上位の日を `hydro_date` でフィルタし、`df_hour_raw` から同日の時間明細を引き、最大/最小/ピーク時刻を取得。  
- `df_summary_peak` を正式に持ち、日別ピーク値・ピーク時刻を保持する。

### 保存・I/O
- 出力: Excel（複数シート）で主テーブルやサマリを出力する。  
- 中間・最終: 各DFをParquetでも保存（粒度細かめ、個別DFごとに保存）し、再利用を容易にする。  
- 列名は英字で統一、値のラベル（例: 水位/流量/雨量）は別メタまたはファイル名で識別。

### 実装ステップ（関数粒度の目安）
1) `load_hourly(path) -> df_hour_raw`  
2) `load_daily(path) -> df_daily_raw`  
3) `aggregate_hourly(df_hour_raw) -> df_hour_daily`  
4) `merge_daily(df_hour_daily, df_daily_raw) -> df_merged`  
5) `add_ranks(df_merged) -> df_merged_with_ranks`  
6) `add_ikyo(df_merged_with_ranks, source_col=...) -> df_merged_with_ikyo`（位況に使う列を指定）  
7) `build_peaks(df_hour_raw) -> df_summary_peak`（正式に持つ）  
8) `export(df_merged_with_ikyo, df_summary_peak, ...)`（Excel複数シート + Parquet、丸めは小数2位）

### 要確認事項（決めてもらいたい点）
- Excelシート構成: 主テーブル、ピークサマリ、年サマリなどをどのシート名・順で出すか（現状は後で決定でOK）。  
- Parquet保存: 個別DFごとに保存（例: `parquet/df_hour_raw.parquet`, `parquet/df_hour_daily.parquet`, `parquet/df_merged.parquet`, `parquet/df_summary_peak.parquet` で進める）。  
- 丸め: 基本は小数2位で統一。特定列だけ別桁数にする希望があれば指示。
