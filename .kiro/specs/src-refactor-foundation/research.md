# Research & Design Decisions

## Summary
- **Feature**: src-refactor-foundation
- **Discovery Scope**: Extension（既存コードの大規模リファクタリング）
- **Key Findings**:
  - `src/main_datetime.py` にGUI、HTTP、Excel出力が集中しており、層分離が最優先の改修対象である。
  - `src/datemode.py` と `src/main_datetime.py` の両方で `throttled_get` を共有しているが再利用APIやエラー契約が未定義である。
  - `.kiro/steering` が未整備のため、リファレンスとなる組織共通アーキテクチャが無く、プロジェクト内で標準を策定する必要がある。

## Research Log

### 現行アーキテクチャの整理
- **Context**: どこまで単一ファイルに処理が集中しているか確認する必要があった。
- **Sources Consulted**: `src/main_datetime.py`, `src/datemode.py`, `scripts/online_smoke_test.py`
- **Findings**:
  - GUIイベント処理、HTTPリクエスト、Excel生成、ログ出力が `WWRApp` に同居している。
  - CLI（`src/__main__.py`）とGUIが`process_data_for_code`を直接呼び出し、責務境界がない。
  - HTTP待機や例外メッセージの表現がモジュールにより異なる。
- **Implications**: 層ごとのモジュール切り出しと共通テレメトリAPIが必須。

### HTTPアクセス方針
- **Context**: `throttled_get` の設計をどこまで拡張できるか検証。
- **Sources Consulted**: `src/datemode.py`, `src/main_datetime.py`
- **Findings**:
  - グローバルカウンタ＋スレッドロックで待機を制御しているが、設定値や例外型がハードコーディングされている。
  - CLI/GUIで異なるログメッセージが混在。
- **Implications**: ConfigurableなHTTPクライアントラッパーと監査用イベントを設計する必要。

### データ加工とExcel出力
- **Context**: pandas/xlsxwriter依存をどこで共通化するか判断。
- **Sources Consulted**: `src/main_datetime.py`, `src/datemode.py`, `.github/workflows/build.yml`
- **Findings**:
  - pandasとBeautifulSoupの遅延ロードは導入済みだが、Excelテンプレート出力が各処理で重複。
  - PyInstallerワンファイル配布を想定しており、遅延ロード戦略を壊さないようにする必要。
- **Implications**: ドメイン層でデータ加工APIを新設し、UI層は結果ファイルパスのみを扱う構造が望ましい。

## Architecture Pattern Evaluation
| Option | Description | Strengths | Risks / Limitations | Notes |
|--------|-------------|-----------|---------------------|-------|
| レイヤード + ポート/アダプタ | UI/CLI→アプリケーションサービス→ドメイン→インフラ(HTTP, Excel) | 既存コードを移設しやすく、テスト境界も明瞭 | 既存ファイルの大規模分割が必要で一時的にビルドが不安定 | リファクタリングの第一段階として採用 |
| 既存構成の最適化のみ | 現行ファイルに軽微な関数抽出を行う | 作業コストは低い | 根本的な責務分離が達成できず再発リスク大 | 採用しない |

## Design Decisions
### Decision: 層分離とCoreサービスの新設
- **Context**: 要件1で定義されたモジュール境界を担保するため。
- **Alternatives Considered**:
  1. 既存関数を整理するのみ
  2. レイヤー分割とサービスAPIの導入（採用）
- **Selected Approach**: `core.fetch`, `core.export`, `common.http`, `ui.cli`, `ui.gui` の5ブロックを定義し、API境界で引数/戻り値の型を固定。
- **Rationale**: CLIとGUIが同じ処理を呼び出せるようにしつつ、HTTPやExcel出力を交換可能にする。
- **Trade-offs**: 初期コストは高いが、後続の機能追加や自動テストが容易になる。
- **Follow-up**: 実装フェーズで型ヒントと例外クラスの標準化が必要。

### Decision: HTTPクライアント設定の集約
- **Context**: 要件2で示された待機・リトライポリシーを一元化する必要。
- **Alternatives Considered**:
  1. `throttled_get`をそのまま利用
  2. ConfigurableなHTTPクライアントモジュールを用意（採用）
- **Selected Approach**: `common.http.ThrottledClient`（クラス or プロトコル）を設計し、待機・ログ・例外をここで統制。
- **Trade-offs**: 既存コードの呼び出し箇所をすべて置換する必要。

## Risks & Mitigations
- 大規模ファイル分割に伴うリリースリスク → 段階的マージと自動テスト整備
- Tkinter依存が依然として重い → 遅延ロード＋UI層限定importで影響最小化
- `.kiro/steering` 不在による標準不足 → デザイン文書で内部標準を定義し、以後の機能で流用

## References
- `src/main_datetime.py`, `src/datemode.py`
- `.github/workflows/build.yml`（PyInstaller配布の制約確認）
