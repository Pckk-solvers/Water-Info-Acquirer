## 詳細設計（JMA年存在判定 + WaterInfo Parquet保存ガード）

## 1. 目的
- JMA: 観測所ごとの「年存在判定」を追加し、不要な年/月リクエストを減らす。
- WaterInfo: リクエスト最適化は行わず、`有効値ゼロ年` の Parquet 保存を抑止する。

## 2. 対象/非対象
### 対象
- `src/river_meta/services/rainfall.py`
- `src/river_meta/rainfall/` 配下に追加する JMA 年判定モジュール
- GUI/ログ文言（進捗とフォールバック可視化）
- `run_rainfall_analyze()` 経路への統合

### 非対象
- WaterInfo の URL 生成ロジック変更（`src/water_info/service/flow_fetch.py`）
- WaterInfo の年存在判定適用
- 年一覧キャッシュ
- `run_rainfall_collect()` 経路への適用

## 3. 現行課題（実装ベース）
1. JMA
- 取得は年×月で順次ループし、月ごとに `parquet_exists` を確認している。
- 「その観測所に存在しない年」を事前に除外していない。

2. WaterInfo
- 年単位で `parquet_exists` は見ている。
- `part.records` が1件以上なら保存するため、`rainfall_mm=None` しか無い年でも Parquet が作られうる。

3. JMA `index.php` の実ページ確認（2026-03-05）
- 年は `<select>` ではなく `a[href]` の `index.php?...year=YYYY...` リンクとして列挙される。
- 例: `prec_no=62, block_no=0604` で `1976..2026` の年リンクを確認。
- 無効な `block_no` 指定時は観測所文脈が保持されないページが返るケースがあるため、文脈一致判定が必要。

## 4. 追加コンポーネント設計（JMA）
### 4.1 新規モジュール
- 追加先: `src/river_meta/rainfall/jma_availability.py`

### 4.2 公開関数
1. `build_jma_index_url(prec_no: str, block_no: str) -> str`
- 役割: `index.php` 用URLを一元生成
- 仕様:
  - base: `https://www.data.jma.go.jp/stats/etrn/index.php`
  - query: `prec_no`, `block_no`, `year`, `month`, `day`, `view`
  - `year/month/day/view` は空文字
  - `prec_no` は2桁化（`zfill(2)`）
  - `block_no` は trim のみ（内部0埋めしない）

2. `fetch_available_years_hourly(prec_no: str, block_no: str, *, timeout: float = 10.0) -> JmaAvailabilityResult`
- 役割: JMA index ページを取得して、年候補を抽出し、判定結果を返す
- 戻り値（推奨）:
  - `JmaAvailabilityResult(status, years, reason)`
  - `status`: `success_with_years` / `success_empty` / `indeterminate`
- 手順:
  - URL生成
  - `requests.get(...)`
  - HTML 解析（観測所文脈一致確認 + 年候補抽出）
  - 判定結果を返却
- 補足:
  - 例外送出で分岐を作るのではなく、戻り値でフォールバック判断を明示する。

3. `parse_available_years_from_index_html(html: str, *, prec_no: str, block_no: str) -> tuple[set[int], bool]`
- 役割: HTML 解析専用（純粋関数）
- 抽出対象:
  - `a[href]` の `index.php?...year=YYYY...` クエリ
  - 抽出対象は `prec_no`/`block_no` が指定値と一致するリンクに限定
- 戻り値:
  - `set[int]`: 抽出年集合
  - `bool`: 指定観測所文脈がページ上で確認できたか
- 仕様:
  - `1900 <= year <= 2100` の範囲でフィルタ
  - 重複除去して返却
  - 観測所文脈不一致時は `years` があっても `indeterminate` 扱いにする

### 4.3 判定結果モデル（推奨）
- `JmaAvailabilityResult`:
  - `status: Literal["success_with_years", "success_empty", "indeterminate"]`
  - `years: set[int]`
  - `reason: str`（ログ出力用。例: `http_error`, `parse_error`, `station_context_mismatch`）

## 5. 既存フロー統合設計（JMA）
### 5.1 統合点
- `src/river_meta/services/rainfall.py`
- `run_rainfall_analyze()` 内の JMA 観測所ループ前後

### 5.2 挙動
1. ユーザー指定年 `years` を観測所ごとに取得
2. `fetch_available_years_hourly(...)` で `JmaAvailabilityResult` を取得
3. `status == indeterminate` の場合は `years` をそのまま使用（フォールバック）
4. `status == success_*` の場合は `target_years = years ∩ available_years`
5. `target_years` が空ならその観測所はスキップ
6. 各 `target_years` で既存 `_fetch_jma_year_monthly(...)` を実行
7. 月次の `parquet_exists` 判定は既存のまま

### 5.3 フォールバック
- `indeterminate`（通信失敗/解析失敗/観測所文脈不一致）の観測所は従来フローに戻す
- 全体停止しない（観測所単位で縮退）
- `success_empty` は失敗ではなく、当該観測所を年スキップする正常系とする

## 6. WaterInfo 保存ガード設計
### 6.1 統合点
- `src/river_meta/services/rainfall.py` の `_fetch_waterinfo_year()`

### 6.2 挙動
1. 既存どおり `parquet_exists(..., "water_info", ...)` を先に確認
2. 未存在なら `_collect_waterinfo_with_resolved(...)` を実行
3. `part.records` のうち `record.rainfall_mm is not None` の件数を算出
4. 有効件数 `0` の場合:
  - `save_records_parquet(...)` は呼ばない
  - ログ: `有効値なしのため保存スキップ`
  - `None` を返却
5. 有効件数 `>=1` の場合:
  - 既存どおり保存し、DataFrame化して返却

### 6.3 注意点
- `records` 全件を捨てるのではなく「保存判定」にのみ使う。
- 後方互換を優先し、アダプタやURL生成は変更しない。

## 7. ログ/UX設計
1. JMA
- 観測所単位サマリ:
  - `指定年数 -> 判定後年数`
  - `判定結果: success_with_years/success_empty/indeterminate`
- 全体サマリ:
  - `削減年数（合計）`
- 失敗時:
  - `JMA年判定失敗 -> 従来モード`

2. WaterInfo
- 保存スキップ時:
  - `観測所=... 年=... 有効値なしのためParquet保存スキップ`

## 8. エラー処理方針
1. JMA 年判定
- 通信失敗/パース失敗/観測所文脈不一致: `indeterminate` として警告ログ + 従来モード

2. WaterInfo 保存判定
- `sum(record.rainfall_mm is not None for record in part.records)` で判定する。
- 判定失敗を想定しない（`RainfallRecord` 前提）。失敗時は上位例外として扱う。

## 9. 変更対象ファイル
1. 新規
- `src/river_meta/rainfall/jma_availability.py`
- `docs/dev/jma-hourly-availability-optimization/design.md`

2. 更新
- `src/river_meta/services/rainfall.py`
- `docs/dev/jma-hourly-availability-optimization/task.md`

## 10. テスト設計
1. JMA 単体
- URLビルダーが期待クエリを生成
- HTMLパーサが「同一観測所リンクのみ」で年集合を抽出
- 観測所文脈不一致時に `indeterminate` 判定になる

2. JMA 結合
- 年フィルタが適用され、不要年が処理対象外になる
- `indeterminate` 時に従来フローで継続
- `success_empty` 時に観測所が正常スキップされる

3. WaterInfo 単体/結合
- 有効件数0: Parquet未作成
- 有効件数1以上: Parquet作成
- 既存Parquetあり: 取得せず読み込み
