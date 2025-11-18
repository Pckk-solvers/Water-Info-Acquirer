# Implementation Plan

- [x] 1. common.http ThrottledClient と TelemetryService の基盤を整備 (P)
- [x] 1.1 ThrottledClient をクラス化し、待機/リトライ/ログを設定可能なAPIで提供する (P)
  - RetryPolicy・RequestBudget・HttpEventなどの型を設計し、requests呼び出し箇所が設定値を注入できるようにする
  - 429/5xx時の再試行、最大試行回数超過時の例外内容、TelemetryServiceへ送信するイベントpayloadを定義する
  - _Requirements: 1.1, 2.1, 2.2, 2.3, 2.4_
- [x] 1.2 TelemetryServiceを設計し、CLI/GUIの診断ログとGUI進捗通知を共通化する (P)
  - TelemetryEventの種別（Start/Progress/Success/Failure）とpayloadを定義し、ログ書式やGUI通知用キューの仕組みを明示する
  - HTTPおよびAppServiceから送られるイベントの保持先（logging/queue等）を決め、IDやタイムスタンプを含むメタ情報を標準化する
  - _Requirements: 1.4, 2.3, 4.3, 4.4_

- [x] 2. core.fetch / core.parser を導入してデータ取得と正規化を分離
- [x] 2.1 FetchService API を定義し、期間計算・URL生成・HTTP呼び出しを集約する
  - FetchRequest/Response構造体を用意し、GUI/CLI双方から同じサービスAPIでデータ取得を指示できるようにする
  - 月リスト生成ロジック、URLのフォーマット、TelemetryServiceへのイベント送信をサービス内部に移す
  - _Requirements: 1.1, 1.2, 3.1, 3.4_
- [x] 2.2 Parser/Normalizer を作成し、BeautifulSoup + pandas の処理を遅延ロードしつつドメインレコードへ変換する
  - HourlyRecordなどのデータモデルを定義し、欠損値や異常値への対応ポリシーを整理する
  - 例外（DataUnavailableErrorなど）を共通クラスにまとめ、UI層が扱いやすいメッセージを得られるようにする
  - _Requirements: 1.1, 1.3, 3.3_

- [x] 3. core.export と WorkbookComposer を再設計して Excel 出力を共通API化
- [x] 3.1 ExportService API を設計し、FetchResponseとExportOptionsからExcelファイルを生成する
  - single_sheetやテンプレート（WH/QD/RD）の切り替えロジックをExportOptionsで表現し、結果ファイルパスのみ返す
  - EmptyExcelWarning等の例外をサービス内で捕捉し、TelemetryServiceへ通知＋AppServiceに伝播させる契約を作る
  - _Requirements: 1.1, 3.1, 3.2, 3.3_
- [x] 3.2 WorkbookComposerを抽象化し、pandas/XlsxWriter実装を差し替え可能にする
  - シートレイアウトやチャート挿入の責務をまとめ、テンプレートごとの設定値をConfigProviderから受け取れるようにする
  - テストモード時はスタブ化したWorkbookComposerでI/Oを行わない挙動を提供する
  - _Requirements: 3.1, 3.4_

- [x] 4. AppService 層を実装し、UI（CLI/GUI）からの入力を統合
- [x] 4.1 ExecutionOptions / UseCaseResult などのデータモデルを定義し、CLI/GUIで共通利用できるようにする
  - dataclass等で期間・コード・モード・single_sheetなどの入力を表現し、バリデーションルールをまとめる
  - DomainErrorやValidationErrorをAppServiceで統一的に扱えるようにする
  - _Requirements: 1.1, 1.2, 3.2, 4.2_
- [x] 4.2 AppService.execute を実装し、FetchService/ExportService/TelemetryService を orchestration する
  - CLI/GUI双方から同じexecute APIを呼び出し、成功/失敗/進捗イベントをTelemetryへ流す
  - テストモードやHTTPスタブを注入するための依存性注入ポイントを用意する
  - _Requirements: 1.2, 3.1, 3.4, 4.2, 4.3_

- [x] 5. UI 層 (CLI / GUI) を AppServiceベースで再配線する
- [x] 5.1 CLI エントリ（`python main.py`, `python -m src`）をExecutionOptions生成とAppService呼び出しに刷新
  - argparseメッセージとValidationResult表示を統一し、Telemetryイベントの受取表示もCLI専用フォーマットで行う
  - 既存 `process_data_for_code` 呼び出しをAppServiceの利用へ切り替える
  - _Requirements: 1.4, 4.1, 4.2, 4.3_
- [ ] 5.2 TkinterベースGUIをGUIShellとして再構築し、AppService＋Telemetryと連携
  - 実行ボタン押下でAppService.executeをバックグラウンド呼び出しし、ProgressSignalを受け取ってUI更新
  - エラー時にはTelemetyEvent(Failure)を捕捉し、モーダルまたはログパネルで表示
  - _Requirements: 1.4, 4.1, 4.4_

- [ ] 6. 統合テレメトリとテストカバレッジ
- [ ] 6.1 TelemetryService発行イベントをCLI/GUI/HTTPの各要件と紐付けて確認する
  - HTTPリトライ、GUI進捗、CLIログ出力の各イベントが設計通り発生することをログやUIで検証
  - _Requirements: 2.3, 4.3, 4.4_
- [ ] 6.2 Fetch/Export/AppServiceのユニットテストとスモークテストを整備する
  - HTTPスタブとTelemetryスタブを使い、要件1〜4の主要シナリオ（正常/エラー）を自動化
  - `scripts/online_smoke_test.py` をAppService経由の呼び出しに更新し、CIで回せるようにする
  - _Requirements: 1.1, 2.1, 3.1, 4.1_
