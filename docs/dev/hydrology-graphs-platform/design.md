# Hydrology Graphs Platform 詳細設計

## 1. 目的

- Parquet 契約準拠データを入力に、全6種グラフを PNG で生成する。
- GUI で設定したスタイルを JSON に保存・再利用し、複数観測所 × 複数基準日のバッチ生成を可能にする。
- 既存ランチャーから独立画面として起動できる構成にする。

## 2. 実装配置

- 新規パッケージ: `src/hydrology_graphs`
- 想定サブ構成:
  - `ui/` GUI 画面・入力・結果表示
  - `services/` 生成ユースケース・バッチ制御
  - `domain/` モデル・バリデーション・窓切り出し規則
  - `io/` Parquet 読込、基準線読込、スタイル読込、PNG 出力
  - `launcher_entry.py` ランチャー接続用エントリポイント

補足:
- 既存 `river_meta.rainfall` からは独立させる。
- 入力契約は以下を正本とする。
  - Parquet: `parquet-contract.md`
  - 基準線: `threshold-contract.md`
  - スタイル: `style-contract.md`

## 3. 層ごとの責務

### 3.1 `domain`

- 純粋ロジック（副作用なし）
  - グラフ種別と必要 metric/interval の整合チェック
  - 3日/5日の窓切り出し
  - 欠損判定（欠損許容0）
  - 年最大算出（瞬間最大）
  - 基準線適用キー生成（`source + station_key + graph_type`）

### 3.2 `io`

- 外部I/O
  - Parquet ロード・正規化
  - 基準線 CSV/JSON ロード
  - style JSON ロード/正規化
  - PNG 書き出し

### 3.3 `services`

- ユースケース統合
  - 条件設定・実行タブで使う実行前検証
  - スタイル調整タブで使うプレビュー生成
  - バッチ実行制御（部分成功継続、停止反映）
  - 対象別結果集約

### 3.4 `ui`

- 2タブ GUI の状態遷移と入出力
  - タブ1（条件設定・実行）: 条件設定 + 実行前検証 + バッチ実行 + 結果表示
  - タブ2（スタイル調整）: スタイル調整 + プレビュー
- UI は `services` の返却DTOのみを扱う。

## 4. 主要I/F仕様

### 4.1 事前検証ユースケース

入力:

```text
PrecheckInput
- parquet_dir: str
- threshold_file_path: str | None
- graph_types: list[str]
- station_keys: list[str]
- base_dates: list[str]          # YYYY-MM-DD
- event_window_days: 3 | 5
```

出力:

```text
PrecheckResult
- summary:
  - total_targets: int
  - ok_targets: int
  - ng_targets: int
- items: list[TargetCheckResult]

TargetCheckResult
- target_id: str
- source: str
- station_key: str
- graph_type: str
- base_datetime: str | None
- status: "ok" | "ng"
- reason_code: str | None
- reason_message: str | None
```

### 4.2 プレビュー生成ユースケース

入力:

```text
PreviewInput
- parquet_dir: str
- threshold_file_path: str | None
- style_json_path: str | None
- style_payload: dict | None
- source: str
- station_key: str
- graph_type: str
- base_datetime: str | None
- event_window_days: 3 | 5 | None
```

出力:

```text
PreviewResult
- status: "success" | "error"
- reason_code: str | None
- reason_message: str | None
- image_bytes_png: bytes | None
```

### 4.3 バッチ実行ユースケース

入力:

```text
BatchRunInput
- parquet_dir: str
- output_dir: str
- threshold_file_path: str | None
- style_json_path: str | None
- style_payload: dict | None
- targets: list[BatchTarget]

BatchTarget
- source: str
- station_key: str
- graph_type: str
- base_datetime: str | None
- event_window_days: 3 | 5 | None
```

出力:

```text
BatchRunResult
- summary:
  - total: int
  - success: int
  - failed: int
  - skipped: int
- items: list[BatchRunItemResult]

BatchRunItemResult
- target_id: str
- status: "success" | "failed" | "skipped"
- reason_code: str | None
- reason_message: str | None
- output_path: str | None
```

## 5. 処理詳細

### 5.1 条件設定・実行タブ（実行前）

1. Parquet ディレクトリをスキャン
2. 契約違反データ（列不足・許容値外）を除外
3. 観測所ごとの可用 metric / interval / period を集約
4. 経年系で年数不足（<10年）を NG 化
5. イベント系で窓不足（3日/5日、連続時系列）を NG 化
6. `PrecheckResult` を返却し、実行可否を判定

### 5.2 実行処理

1. style JSON 契約検証（`schema_version=1.0`）
2. プレビュー対象の単体描画
3. バッチ対象を順次実行（全体中断しない）
4. 各対象の結果を即時蓄積
5. 停止要求時は未着手のみ中止し、途中結果を返却

### 5.3 グラフ種別と抽出条件

- ハイエトグラフ:
  - `metric=rainfall`, `interval=1hour`
  - 雨量棒 + 累加雨量線
  - Y軸反転対応
- ハイドログラフ（流量）:
  - `metric=discharge`, `interval=1hour`
- 水位波形:
  - `metric=water_level`, `interval=1hour`
- 年最大3種:
  - metric別に年ごとの瞬間最大を算出
  - 10年未満は `insufficient_years`

## 6. エラーコード設計

- `contract_error`
  - 入力契約違反（Parquet/threshold/style）
- `missing_timeseries`
  - 対象窓のデータ不足または欠損
- `insufficient_years`
  - 年最大用データ年数不足
- `threshold_not_found`
  - 対応する基準線が見つからない
- `style_error`
  - style JSON 不正・正規化失敗
- `render_error`
  - 描画または保存失敗

扱い:
- `failed`: 実行継続可能な対象失敗
- `skipped`: 要件上実行不可な対象

## 7. GUI 実装仕様（2タブ）

- 画面仕様の正本は `layout.md`。
- 状態管理キー:
  - `idle`, `scanning`, `ready_config`, `validating`, `ready_style`, `running`, `completed`, `failed_partial`
- イベント:
  - タブ1（条件設定・実行）: `scan`, `validate`, `run`, `stop`
  - タブ2（スタイル調整）: `load_style`, `preview`, `save_style`, `reset_style`
- UI 制約:
  - `running` 中は入力変更禁止
  - タブ1の検証 NG 対象は実行対象から除外
  - タブ2で style 検証 NG の場合は、実行時に保存済み正常スタイルまたはデフォルトへフォールバック

## 8. I/O仕様

### 8.1 入力

- Parquet: `parquet-contract.md` に準拠
- 基準線: `threshold-contract.md` に準拠
- style: `style-contract.md` に準拠

### 8.2 出力

- 形式: PNG
- 出力先: `観測所/グラフ種別/基準日`
- 同名上書き: 有効

## 9. テスト設計

### 9.1 単体

1. Parquet 契約バリデータ
2. 基準線契約バリデータ（CSV/JSON）
3. style 契約バリデータ（v1正規化含む）
4. 日単位固定窓切り出し
5. 欠損判定（欠損1点で NG）
6. 年最大算出（瞬間最大）
7. 基準線マッチング（`source+station_key+graph_type`）

### 9.2 結合

1. 条件設定・実行タブの検証で NG 対象が正しく表示される
2. スタイル調整タブの style 検証 NG でフォールバック動作になる
3. 観測所×基準日のバッチで部分成功継続
4. 停止要求で未着手のみ中止される
5. 出力パス規則が守られる
6. スタイル変更でプレビューが再描画される

## 10. 実装順序（推奨）

1. `domain`（モデル・検証・窓切り出し）
2. `io`（Parquet/基準線/style/PNG）
3. `services`（precheck/preview/batch）
4. `ui`（2タブ状態遷移とイベント接続）
5. `launcher_entry` とランチャー登録
6. テスト整備（単体→結合）
