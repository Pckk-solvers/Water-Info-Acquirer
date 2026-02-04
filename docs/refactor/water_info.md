# water_info リファクタリング方針（案）

## 今回のサマリ: 責務の分離（積載分離）
今回の作業では、UI/サービス/インフラ/ドメインの責務を明確に分ける「積載分離」を実施した。
主な観点は「UIとロジックの分離」「取得処理の共通化」「Excel出力/整形の共通化」「バリデーションの一元化」である。

## 目的
- `src/water_info` の責務分割を明確化し、保守性と変更容易性を高める。
- GUI/取得/整形/出力を分離し、ロジック単体テストが可能な構造にする。
- 既存の入出力仕様（Excelの列/シート/チャート構成）を崩さず、段階的に安全に進める。

## 現状レビュー（主要な課題）
- 単一巨大モジュール（`main_datetime.py`）に UI/ネットワーク/データ整形/Excel出力が混在。
- 日データ・時間データの処理が別モジュールに分かれつつも、重複ロジックが多い（URL生成、観測所名取得、Excel出力など）。
- グローバル変数（`pd`, `BeautifulSoup`, `_REQUEST_COUNTER` など）と遅延importが散在し、依存関係が見えづらい。
- GUIイベントから直接データ取得・出力まで実行されるため、テストや失敗時の分岐が複雑。
- 例外/エラー表示が UI 依存（`show_error`, `_popup`）で、ドメイン層と結合。

## 目指す構造（提案）
- `src/water_info` を以下の責務で分割する。
  - `ui/`: Tkinter 画面構築・イベント・メッセージ表示
  - `domain/`: 取得対象・期間・出力仕様などの値オブジェクト
  - `service/`: 取得フロー・Excel生成の組み立て
  - `infra/`: HTTP取得、スクレイピング、ファイル出力（pandas/xlsxwriter）
- UI から呼ばれるのは「サービス1本（usecase）」に限定。
- 例外はドメイン例外として定義し、UI が捕捉して表示。

## 段階的な進め方（最小リスク）
1. **抽出フェーズ**
   - 既存ロジックから純粋関数・ユーティリティを抽出し、`infra/` に移設。
   - 観測所名取得、URL生成、DataFrame変換、Excel出力を切り出す。
2. **境界フェーズ**
   - `main_datetime.py` の GUI から、取得・出力の処理を `service/` に集約。
   - 例外クラスを共通化（`EmptyExcelWarning` など）し、UIの `show_error/_popup` から分離。
3. **統合フェーズ**
   - `datemode.py` と `main_datetime.py` の共通パスを統合（モード別に戦略化）。
   - 取得処理を 1本化し、`mode_type`/`period_mode` で分岐。
4. **安定化フェーズ**
   - ユニットテスト（pandasユーティリティ、URL生成、期間計算）追加。
   - `postprocess.py` と命名/構造を整合させ、I/O契約を明文化。

## 具体的な分割案（初期）
- `src/water_info/infra/http_client.py`
  - `throttled_get`, `HEADERS`, リトライ設定
- `src/water_info/infra/scrape_station.py`
  - 観測所名取得（HTML解析）
- `src/water_info/infra/excel_writer.py`
  - Excel出力（チャート/シート）
- `src/water_info/service/fetch_hourly.py`
  - 時間データ取得・DataFrame化
- `src/water_info/service/fetch_daily.py`
  - 日データ取得・DataFrame化
- `src/water_info/service/usecase.py`
  - UI から呼ぶ 1 本の実行関数
- `src/water_info/ui/app.py`
  - Tkinter 構築（現在の `WWRApp` を移動）

## リスクと注意点
- Excel出力仕様は既存ユーザーに影響するため最優先で互換維持。
- スクレイピング対象のHTML構造に依存するため、関数単体で最低限のスモークテストを用意。
- `__main__.py` の起動経路（`python -m src.water_info`）を維持。

## 直近の次アクション（提案）
- まず `throttled_get` と `HEADERS` を `infra/http_client.py` に移動。
- `datemode.py` と `main_datetime.py` の両方から参照する形に変更。
- その後、観測所名取得ロジックを共通化。

## テスト先行フェーズ（あなたの選択 3 の具体化）

### 目的
- リファクタリング前に「現状の挙動」を固定化し、移動・分割による回帰を検知可能にする。
- UIを触らずに、取得・整形・出力のコアロジックを検証できる下支えを作る。

### 追加すべきテストの範囲（優先順）
1. **純粋関数/ユーティリティ**
   - `shift_month`, `month_floor` の月跨ぎロジック
   - `_calc_delay` のリクエスト間隔ロジック
2. **URL生成/パラメータの整合性**
   - `process_data_for_code` / `process_period_date_display_for_code` が生成する URL の形式
   - `mode_type` に応じた `KIND` / `DspWaterData` / `DspRainData` の切り替え
3. **DataFrame整形の入出力**
   - 時間データの `display_dt` シフト（+1h）
   - `sheet_year` の決定が「元時刻」に基づくこと
   - 空データ時に `EmptyExcelWarning` が投げられること
4. **Excel出力の最低限スモーク**
   - シート名の有無（`全期間` / `YYYY年` / `summary`）
   - 期待する列名（A列=日時、B列=値 など）

### テスト設計の方針
- **ネットワークはスタブ化**し、HTMLレスポンスを固定する。
  - `throttled_get` をモックして HTML を差し替え。
- **pandas/Excelは軽量確認**に止める。
  - 例: `ExcelWriter` をモックし「シート名」が出ることだけを見る。
- **GUIは対象外**。UI表示はリファクタ後に E2E の位置づけ。

### 具体的に作成するテスト（例）
- `tests/water_info/test_date_utils.py`
  - `shift_month` の +/- 跨年ケース
  - `month_floor` が月初00:00になる
- `tests/water_info/test_http_delay.py`
  - `_calc_delay(0)=0`, `_calc_delay(1)=min`, 上限が `REQUEST_MAX_DELAY`
- `tests/water_info/test_url_build.py`
  - `mode_type='S'/'R'/'U'` で `KIND` と `DspWaterData/DspRainData` が正しい
- `tests/water_info/test_dataframe_logic.py`
  - `display_dt` シフト / `sheet_year` の割当
  - 空データで `EmptyExcelWarning`
- `tests/water_info/test_excel_smoke.py`
  - モック `ExcelWriter` で `全期間` / `YYYY年` / `summary` シートが生成される

### 実行方法
- `uv run pytest -q`

### 完了条件
- 上記テストが `main` の現状実装で通る。
- テストが落ちる場合は「現状仕様」として受け入れるか再確認して仕様化する。

## 進捗（2026-02-04 時点）
- テスト追加完了: `pytest.ini` + `tests/water_info` 配下 (23 tests)
- `infra` への抽出・共通化
  - HTTP 共通化: `infra/http_client.py`
  - 観測所名: `infra/scrape_station.py`
  - URL生成: `infra/url_builder.py`
  - Excel共通: `infra/excel_writer.py`
  - DataFrame共通: `infra/dataframe_utils.py`
  - Excelサマリ共通: `infra/excel_summary.py`
  - HTML/HTTP共通: `infra/http_html.py`
  - 値抽出共通: `infra/scrape_values.py`
  - 取得共通: `infra/fetching.py`
- サービス層導入: `service/usecase.py` を追加し UI からの入口を集約
- UI 分離: `ui/app.py` に `WWRApp` を移動。`main_datetime.py` は起動/ロジックに集中
- domain 導入: `domain/models.py` に Period/Options/WaterInfoRequest を定義
  - Period: 年/月の妥当性検証 + 期間逆転チェック
  - WaterInfoRequest: mode_type 検証
- service 整理: `fetch_one` / `fetch_for_code` / `FetchOutcome` を追加し `fetch_water_info` は request 単位で処理
- UI 表示整理: 入力エラー/処理エラーの表示フォーマットを統一
- UI バリデーション整理: 年月のフォーマットチェックは domain に一本化
- UI 即時バリデーション: フォーカスアウト/選択時に検証し、同一エラーはクールダウン
- UI エラー表示統一: 即時エラーはフォーム内ラベル、実行時ポップアップは使用しない
- UI バリデーションのクリア条件: 入力変更時のみクリア
- テスト追加: `test_domain_validation.py`, `test_usecase_fetch_for_code.py`, `test_scrape_smoke.py`
- いずれも `uv run pytest -q` で通過

## 次の候補
- `service` をユースケース単位に分割（取得/出力を分ける）
- `infra` のスクレイピング部分に簡易スモークテストを追加
- UI側の入力バリデーションと domain バリデーションの二重化を解消
