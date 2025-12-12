# Water Info 後処理 仕様書（現行実装）

## 目的・対象範囲
- 国土交通省の水文データ (_H 系時間データ、_D 系日データ) を後処理し、日次集計・ランク付与・位況算出・ピーク抽出・年次サマリを生成して Excel/Parquet に出力する。
- 対象となる値の種類は水位以外にも流量・雨量などを想定し、列名ゆらぎは読み込み時に吸収する。

## 入力と前提
- 必須: `--hour-file` で指定する _H 系 Excel。
- 任意: `--daily-file` で指定する _D 系 Excel。未指定でも時間データだけで集計・ランク・位況を出力する。
- Excel シート選択: `"全期間"` があれば優先。無ければ `^\d{4}年$` のシートを全件読み込み、連結して日時順に整列。
- 読み込み列: 先頭2列を使用 (`usecols=[0,1]`)。時間は `display_dt`、日次は `datetime` として受け、値列は `value`/`daily_value` に正規化。
- 読み込み時丸め: 値列は `Decimal` + `ROUND_HALF_UP` で小数第3位 (0.001) に量子化。NaN 変換も許容。
- 日付キー: `hydro_date = (display_dt - 1時間).date()` を作成し、1:00〜0:00 を同一日として扱う。

## 日次集計（時間→日）
- グループキー: `hydro_date`。
- `count_non_null`: 非欠損本数を計数。
- 可変分母平均 (`hourly_daily_avg_var_den`): 非欠損本数で割る。`Decimal` で計算し小数第2位に四捨五入。
- 固定分母平均 (`hourly_daily_avg_fixed_den`): 本数が24本のときのみ (欠損0) 上記と同様に平均、24未満なら NaN。
- 出力は `hydro_date` を datetime に戻し、必要に応じて `year` 列を付与。

## 日データとのマージ
- 外部結合キー: `hydro_date`。
- 日データ列 `daily_value` は結合後に小数第2位へ `ROUND_HALF_UP`。
- 並び順: `hydro_date` 昇順。`year` 列を付与。

## ランク付与
- 対象列: `hourly_daily_avg_var_den`, `hourly_daily_avg_fixed_den`, `daily_value` (日データ無し時は前2列のみ)。
- 年ごとに処理。欠損11件以上の年はその列のランクを全て NaN。
- ランク値は丸め後 (小数第2位) を使用し、値降順→タイブレーク (`tie_breaker`) 昇順でソートし 1 から連番を振る。重複値も固有ランクとなる。
- 欠損へのランク: `rank_missing=True` のため、非欠損の次番号から日付昇順で連番を付与。
- 標準版 (`add_ranks`): 閾値あり、タイブレークは `hydro_date`。
- 参考版 (`add_ranks_no_threshold`): 閾値なし。タイブレークは可変分母平均を丸めた `_tie_key` を共有させ、列間の並びを合わせる。
- ランク列名: `rank_var_den`, `rank_fixed_den`, `rank_daily_value`。

## 位況算出
- 基準順位: 豊水位=95, 平水位=185, 低水位=275, 渇水位=355 をベースに 365 日基準で設定。
- ランク補正 `_calc_rank`:
  - `use_scaling=True`: `base_rank/365` を対象年日数 (365/366) へスケールし、欠損日数でさらに縮約 (floor)。欠損>=11 または欠損=全日なら None。
  - `use_scaling=False`: 補正なしで基準順位をそのまま使用。欠損全日なら None。
- 各年・各対象列について非欠損値を降順に並べ、上記順位に対応する値を `ikyo_high/normal/low/drought` (列サフィックスは `var_den`/`fixed_den`/`daily_value`) に格納。ランクが範囲外の場合は NaN。
- 標準版: 欠損閾値・補正あり。参考版: 閾値なし・補正なし。

## ピーク抽出
- `hydro_date` ごとに非欠損の最大値とその `display_dt` を取得。
- 列: `peak_max_value`, `peak_max_time`（時刻は水水DB基準）。値は後段で小数第2位に丸めて出力。

## 年次サマリ
- 年ごとに以下を集計（対象列は日データ有無で変化）。
  - 欠損数: `missing_{suffix}`。
  - 平均: 非欠損を `Decimal` で平均し小数第2位に四捨五入 (`mean_{suffix}`)。
  - 位況値: 各 `ikyo_*_{suffix}` の非欠損先頭値。
  - 1時間値の最大/最小と時刻: `max_hourly_value/time`, `min_hourly_value/time` (値は小数第2位で丸め)。
  - 位況で採用した順位: `rank_used_ikyo_*_{suffix}` (スケーリングと閾値は標準/参考で切り替え)。
- Excel 出力時は転置し、1列目を `項目` として指標名、以降に年次データを配置。

## 出力仕様
- Excel (デフォルトシート名):
  - `main`: 標準ランク/位況。日データ有りの場合は日データ列込み、無しの場合は時間集計のみ。
  - `main_raw_rank`: 閾値なし・補正なしの参考ランク/位況。
  - `peaks`: 日別ピーク値と時刻。
  - `year_summary`: 年次サマリ（標準版、転置形式）。
  - `year_summary_raw`: 年次サマリ（参考版、転置形式）。
- 列名は Excel 出力時に日本語へリネームする（例: 日付, 日平均（可変分母）, ランク（固定分母）, 位況渇水位…）。数値は書き出し前に小数第2位へ `ROUND_HALF_UP`。
- Parquet: `--out-parquet` 指定時のみ出力。
  - 日データあり: `df_hour_raw`, `df_hour_daily`, `df_merged` (位況込み), `df_summary_peak`。
  - 日データなし: `df_hour_raw`, `df_hour_daily`, `df_summary_peak`。

## 設定ファイル
- オプション `--config` で JSON ファイルを指定可能。キーは CLI 引数の `dest` 名 (`hour_file`, `daily_file`, `out_excel`, `out_parquet`, シート名系など) をそのまま使う。
- 読み込んだ値は引数のデフォルトとして適用され、同じ項目を CLI で渡した場合は CLI が優先される。
- 不明キーは警告を出して無視。

## ログとエラーハンドリング
- 主な処理ステップで `[INFO]` ログを標準出力に出す（読込、集計、ランク付与、位況算出など）。メッセージは日本語。
- シート未検出など致命的条件は `ValueError` を送出して終了。

## 実行例
- 日データあり:
  ```
  uv run python -m water_info.postprocess ^
    --hour-file water_info/303031283303010_木原_2020年1月-2021年12月_WH.xlsx ^
    --daily-file water_info/303031283303010_木原_2020年1月-2021年12月_WD.xlsx ^
    --out-excel out/postprocess_result.xlsx ^
    --out-parquet out/parquet_result
  ```
- 日データなし:
  ```
  uv run python -m water_info.postprocess ^
    --hour-file water_info/303031283303010_木原_2020年1月-2021年12月_WH.xlsx ^
    --out-excel out/postprocess_hour_only.xlsx
  ```
