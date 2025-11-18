# Requirements Document

## Introduction
Water-Info-Acquirer の既存 `src` ディレクトリは処理・UI・HTTPアクセスが混在しており、再利用やテストが困難になっている。本仕様はリファクタリング着手前に求められる振る舞いを整理し、層ごとの責務や品質基準を明文化する。

## Requirements

### Requirement 1: モジュール階層と責務分離
**Objective:** As a 開発者, I want src配下の責務境界を明確化し、so that 機能追加や変更時の影響範囲を制御できる

#### Acceptance Criteria
1. The Refactored Codebase shall ドメイン処理層・UI層・共有ユーティリティ層を明示したディレクトリ構成を採用する。
2. When 新規機能のドメイン処理を追加する, the Refactored Codebase shall UI層から独立したモジュールに配置できるようにする。
3. If モジュールが複数の層にまたがる依存を持つ, the Architecture Guidelines shall 依存方向を単一方向（UI→ドメイン→共有）に矯正する。
4. The Refactored Codebase shall 既存ファイルのうち混在していた責務を専用モジュールへ移設した証跡を残す。

### Requirement 2: HTTPアクセスとリトライ共通化
**Objective:** As a 開発者, I want HTTPアクセスの待機・リトライ方針を一元化し, so that 取得安定性と保守性を両立できる

#### Acceptance Criteria
1. When データ取得モジュールがHTTPリクエストを発行する, the HTTP Access Layer shall 共通のユーザーエージェント・ヘッダー・ログ出力を適用する。
2. When レート制限応答や一時的な 429/5xx が発生する, the HTTP Access Layer shall パラメータ化された待機時間と最大リトライ回数で再試行する。
3. If リトライ上限を超えても失敗する, the HTTP Access Layer shall 明確な例外と再現に必要なメタ情報を呼び出し元へ返す。
4. While 並列モジュールが同時にリクエストを組み立てる, the HTTP Access Layer shall スレッドセーフに待機インデックスを管理する。

### Requirement 3: データ加工と出力責務の独立
**Objective:** As a 開発者, I want データ変換とExcel出力をUIから切り離し, so that CLI/GUIの両方で同一処理を再利用できる

#### Acceptance Criteria
1. The Data Processing Module shall 観測所・期間・モードに応じたURL生成、解析、データ整形、Excelテンプレート出力をひとつの公開API経由で提供する。
2. When CLIまたはGUIがデータ出力を要求する, the Data Processing Module shall 依存注入された設定値（単一/複数シート等）を尊重する。
3. If データ欠損やフォーマット異常が検出される, the Data Processing Module shall UI層で表示可能な専用例外を送出する。
4. Where テストモードが有効化される, the Data Processing Module shall 外部I/Oをスタブ化できる拡張ポイントを提供する。

### Requirement 4: 実行エントリとテレメトリ
**Objective:** As an オペレーター, I want CLI/GUI/自動化スクリプトで一貫した起動・監視を行い, so that 運用時の不具合を迅速に把握できる

#### Acceptance Criteria
1. The Entry Module shall `python main.py` と `python -m src` の双方で同一の引数体系を受け付ける。
2. When 実行モードや期間パラメータがユーザー操作で変更される, the Entry Module shall バリデーション結果とヘルプテキストを統一したメッセージ形式で表示する。
3. If バッチ／GUI実行が失敗する, the Telemetry Layer shall 取得URL・期間・モードなどの診断情報をログに残す。
4. While GUIがバックグラウンド処理を実行する, the Telemetry Layer shall スレッドセーフに進捗と完了結果を通知できる仕組みを提供する。
