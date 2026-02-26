# Rainfall JMA Refactor Materials

## 目的
- JMA取得処理の責務競合を解消し、処理速度と保守性を上げる。
- マルチエージェントで安全に並行実装できる分割単位を明確化する。

## 現状フロー（要約）
1. `run_rainfall_analyze()` が source/period/station を解決。
2. JMAは `station x year` で `_fetch_jma_year_monthly()` を実行。
3. 月単位で Parquet キャッシュ判定し、未キャッシュのみ `fetch_jma_rainfall()`。
4. `fetch_jma_rainfall()` は `Fetcher.schedule_fetch()` で日次HTMLを順次取得し、parserで正規化。
5. 月次Parquetを保存し、年次結合して timeseries/annual max を作成。

## 競合・重複ポイント
- interval -> frequency 変換が `jma_adapter` と `fetcher` に重複。
- `cancelled` 判定ユーティリティがサービス層とadapter層で重複。
- parserの sample date 解決ロジックが daily/hourly/10min で重複。
- 出力処理（timeseries -> Excel/Chart）が analyze/generate で重複。

## 最適化候補（優先順）
1. 取得I/Oの並列化
- `Fetcher.schedule_fetch()` の日次直列取得を並列化（観測所単位/日付バッチ単位）。

2. Record生成コスト削減
- `df.to_dict(orient="records")` の全展開を削減（`itertuples` などへ変更）。

3. JMA年次再結合コスト削減
- 月次Parquet再読込の回数削減（メタインデックス/増分結合）。

4. parser生成・HTML探索の使い回し
- parser構築とテーブル探索の固定コストを削減。

5. parser実装統合
- A1/S1で重複する parse 骨格を統合し、変更耐性を向上。

## マルチエージェント分割案
### Agent A: Orchestration
- 対象: `src/river_meta/services/rainfall.py`
- 目的: `collect/analyze/generate` 共通責務の抽出と流れ整理。

### Agent B: Fetching
- 対象: `src/jma_rainfall_pipeline/fetcher/`
- 目的: 並列取得、再試行、レート制御の最適化。

### Agent C: Adapter
- 対象: `src/river_meta/rainfall/jma_adapter.py`
- 目的: datetime正規化とRecord生成の軽量化。

### Agent D: Parser
- 対象: `src/jma_rainfall_pipeline/parser/`
- 目的: 共通ロジック統合（sample date, table parse骨格）。

### Agent E: Cache/Store
- 対象: `src/river_meta/rainfall/parquet_store.py`, `src/river_meta/services/rainfall.py`
- 目的: 月次->年次のI/O最適化、再利用方針整理。

## 実施順（推奨）
1. Agent A（インタフェース境界を先に固定）
2. Agent B/C（I/Oと変換のボトルネック改善）
3. Agent D（重複統合）
4. Agent E（保存戦略最適化）

## 完了条件
- 既存GUI機能を壊さず、同一入力で同一出力（差分は性能のみ）。
- キャンセル時の状態遷移と出力整合を保持。
- 実行時間/メモリのベースライン比較結果を残す。
