## タスク分解（詳細設計反映版）

### Phase 1: JMA 年判定モジュール実装
1. ファイル追加
- `src/river_meta/rainfall/jma_availability.py`

2. URLビルダー実装
- `build_jma_index_url(prec_no, block_no)` を実装。
- `prec_no` 2桁化、`block_no` trim、`year/month/day/view` 空文字の固定クエリを生成。

3. HTMLパーサ実装
- `parse_available_years_from_index_html(html, prec_no, block_no)` を実装。
- `index.php?...year=YYYY...` リンクから、同一 `prec_no`/`block_no` の年だけを抽出する。
- 観測所文脈一致フラグ（bool）を返す。

4. 取得関数実装
- `fetch_available_years_hourly(...)` を実装。
- 通信 + パースの責務を集約。
- 戻り値を `JmaAvailabilityResult`（`success_with_years` / `success_empty` / `indeterminate`）で統一する。

### Phase 2: JMA フロー統合
1. `services/rainfall.py` に統合
- 適用先は `run_rainfall_analyze()` の JMA 処理に限定する。
- 観測所ごとに `status` を判定し、`success_*` のときだけ `指定年 ∩ 利用可能年` を計算。
- 空集合なら観測所スキップ。

2. 既存処理維持
- `_fetch_jma_year_monthly()` の月次 `parquet_exists` 判定はそのまま利用。

3. フォールバック
- `indeterminate` 時は観測所単位で従来フローへ戻す。
- 失敗を全体停止にしない。

### Phase 3: WaterInfo Parquet 保存ガード
1. `_fetch_waterinfo_year()` 改修
- `part.records` から `rainfall_mm is not None` 件数を算出。
- 0件なら `save_records_parquet(...)` を呼ばずにスキップ。
- 1件以上なら従来どおり保存。

2. 変更禁止の確認
- `src/water_info/service/flow_fetch.py` の URL生成には手を入れない。
- WaterInfo 年判定最適化は今回入れない。

### Phase 4: ログ/UX調整
1. JMA サマリログ
- 観測所ごとの `指定年数 -> 判定後年数`
- 観測所ごとの判定結果 (`success_with_years` / `success_empty` / `indeterminate`)
- 全体削減年数

2. フォールバックログ
- `indeterminate` 時に従来モード継続を明示

3. WaterInfo スキップログ
- 有効値0件で保存スキップした理由を出力

### Phase 5: テスト
1. JMA 単体
- URLビルダーのクエリ検証
- 年抽出パーサ検証（同一観測所リンクのみ抽出）
- 観測所文脈不一致で `indeterminate` になること

2. JMA 結合
- 年フィルタ適用で処理年が減ること
- `indeterminate` 時フォールバック
- `success_empty` 時スキップ

3. WaterInfo 結合
- 有効値0件で Parquet 非作成
- 有効値ありで Parquet 作成
- 既存Parquetありで取得スキップ

## 実装順
1. Phase 1
2. Phase 2
3. Phase 3
4. Phase 4
5. Phase 5

## リスク/対策
1. JMA ページ構造変更
- 解析失敗を許容し、従来フローへフォールバック。

2. 機能境界の再混同
- JMA 新規機能は `jma_availability.py` に閉じ、WaterInfo 既存処理には影響を与えない。
