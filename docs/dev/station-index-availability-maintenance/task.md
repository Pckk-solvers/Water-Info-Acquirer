# 観測所インデックス可用年メンテナンス タスク

## Phase 1: 要素技術の整理

1. JMA 可用年取得ロジックの棚卸し
- `jma_availability.py` の再利用範囲を確定する
- `indeterminate` 時の扱いを決める
- PDF 補完処理とは責務を分離する

2. WaterInfo 可用年取得ロジックの棚卸し
- `service.py` / `parser_availability.py` から再利用する最小関数を決める
- `hourly` / `daily` の両対応前提で取得関数を整理する

3. JSON スキーマ確定
- 追加キー
- 空値表現
- `availability_updated_at` を各観測所に保持する前提で確定する

## Phase 2: 更新スクリプト設計

1. JMA 更新フローを分離
- PDF 補完と URL ベース可用年補完を別責務として整理する
- 必要なら `update_jma_station_availability.py` を新設する

2. WaterInfo 更新フローを分離
- ベース観測所生成と可用年補完を別責務として整理する
- `update_waterinfo_station_availability.py` を新設する

3. モード整理
- `rebuild`
- `update` / `resume`
- 部分更新オプション

## Phase 3: 実装

1. WaterInfo 可用年更新の実装
- `hourly` / `daily` の availability 取得関数を切り出す
- JSON マージ処理を入れる
- 途中保存と失敗継続を入れる

2. JMA 可用年更新の実装
- 既存 index JSON を読み込む
- 観測所ごとに可用年を取得する
- `indeterminate` の既存値維持を実装する

3. 統合ランナー更新
- `refresh_station_indexes.py` に可用年更新ステップを追加する
- `--target` / `--dry-run` / `--keep-going` の整合を取る

## Phase 4: 利用側反映

1. JMA 利用側
- JSON の `available_years_hourly` を優先参照する
- オンライン availability 判定を移行期間の暫定フォールバックへ下げる

2. WaterInfo 利用側
- JSON の `available_years_hourly` / `available_years_daily` を使った年絞り込みを検討・実装する

3. GUI / resolver 反映
- 必要なら観測所情報表示で年リストから可用年表示を組み立てる

## Phase 5: テスト

1. 解析テスト
- availability HTML から年リストを取れること
- 欠年が保持されること

2. マージテスト
- 既存 JSON に追加キーを壊さず反映できること
- 空値・失敗時の挙動が想定どおりであること

3. ランナーテスト
- `refresh_station_indexes.py` のコマンド構成
- `dry-run`
- `keep-going`

4. 回帰
- `uv run pytest tests/river_meta -q`

## Phase 6: 運用

1. 実行手順の固定
- `uv run scripts/refresh_station_indexes.py ...` を定期更新入口として確立する

2. ドキュメント更新
- `docs/dev/index.md` の導線追加
- 必要なら運用手順書を別紙化する

3. 将来タスク
- JSON ベースで完全に年絞り込みする
- オンライン availability 判定の撤去
- Parquet 実績年との比較診断ツール追加
