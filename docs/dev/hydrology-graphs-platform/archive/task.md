# Hydrology Graphs Platform タスク分解

## 実行方針（粒度）

- 1タスクは 0.5〜1.5日で完了・レビュー可能な粒度にする。
- 依存順は `domain -> io -> services -> ui -> launcher` を固定する。
- 各タスクに完了条件（DoD）を持たせる。

## WP1: Domain モデルと判定ロジック

1. graph_type と metric/interval 要件判定を実装
2. 日単位固定窓切り出し（3日/5日）を実装
3. 欠損許容0判定を実装
4. 年最大（瞬間最大）算出を実装
5. 基準線適用キー生成（`source + station_key + graph_type`）を実装

DoD:
- 単体テストで 3/5日窓、欠損1点、10年未満判定の境界ケースを通過

## WP2: Parquet IO + 契約検証

1. Parquet読込と列正規化を実装
2. `../current/parquet-contract.md` 準拠の検証（必須列/許容値/時刻）を実装
3. 契約違反を対象単位で `contract_error` に変換

DoD:
- 契約違反レコードが全体中断せず対象単位で除外/失敗扱いになる

## WP3: Threshold IO + 契約検証

1. 基準線 CSV/JSON 読込を実装
2. `../current/threshold-contract.md` 準拠の検証を実装
3. `priority` 優先規則と `enabled=false` 除外を実装
4. `source+station_key+graph_type` マッチングを実装

DoD:
- 重複キー時の優先挙動が再現できる

## WP4: Style IO + 契約検証/正規化

1. style JSON 読込を実装
2. `style-contract.md` 準拠の検証（`schema_version=2.0`）を実装
3. 必須9キー（イベント3種×3日/5日 + 年最大3種）の検証を実装
4. `common` / `variants` の拒否を実装
5. 保存時に正規キーのみ出力する処理を実装

DoD:
- `schema_version=2.0` と必須9キーを満たさない入力は拒否される

## WP5: Service - Precheck（条件設定・実行タブ）

1. `PrecheckInput/PrecheckResult` DTO を実装
2. 実行前検証ユースケースを実装
3. 検証結果に基づく実行対象確定ロジックを実装

DoD:
- NG対象に `reason_code` が必ず付与される

## WP6: Service - Preview（スタイル調整タブ）

1. `PreviewInput/PreviewResult` DTO を実装
2. スタイル読込/保存時の style 検証を実装
3. 単体プレビュー描画を実装

DoD:
- style 不正時は `style_error`、正常時は PNG bytes を返す

## WP7: Service - BatchRun（条件設定・実行タブ）

1. `BatchRunInput/BatchRunResult` DTO を実装
2. 対象順次実行を実装（部分成功継続）
3. 停止要求（未着手のみ中止）を実装
4. 対象別結果集約を実装

DoD:
- 停止後も既完了対象の結果が保持される

## WP8: UI - タブ1（条件設定・実行）

1. 条件設定フォームを実装
2. 実行前検証結果テーブルを実装
3. 実行/停止/結果一覧/ログ表示を実装

DoD:
- タブ1で条件設定から実行完了確認まで完結できる

## WP9: UI - タブ2（スタイル調整）

1. スタイル編集UIを実装
2. プレビュー表示を実装（デバウンス再描画）
3. スタイルJSON読込/保存/初期化を実装
4. 実行時に最新スタイルが適用される連携を実装

DoD:
- 調整したスタイルがプレビューと実行の双方に反映される

## WP10: Launcher 統合 + 回帰

1. `launcher_entry.py` を追加
2. `app_registry` への登録を追加
3. 既存ランチャー遷移仕様との整合確認

DoD:
- ランチャーから本機能を起動できる

## テスト計画

### 単体

1. 窓切り出し（3日/5日）
2. 欠損判定（欠損1点でNG）
3. 年最大算出（瞬間最大）
4. Parquet契約検証
5. Threshold契約検証
6. Style契約検証/正規化
7. 基準線マッチング（`source+station_key+graph_type`）

### 結合

1. タブ1の検証でNG対象が正しく除外される
2. タブ2 style 検証NGで `style_error` が返る
3. バッチ実行で部分成功継続する
4. 停止要求で未着手のみ中止される
5. 出力構成（`観測所/グラフ種別/基準日`）が守られる
5. 出力構成（`<output_dir>/<station_key>/<graph_type>/<base|annual>/graph.png`）が守られる
6. スタイル変更でプレビュー再描画される

## 実装順序

1. WP1
2. WP2
3. WP3
4. WP4
5. WP5
6. WP6
7. WP7
8. WP8
9. WP9
10. WP10

## リスクと対策

1. 契約解釈の揺れ
- 契約文書を正本にして、検証ロジックは `io` に集約する。

2. UI と service の責務混在
- UIはDTO構築と表示に限定し、判定処理を持たせない。

3. 長時間バッチ時の操作性低下
- 停止機能と進捗更新を先に実装して体感品質を担保する。
