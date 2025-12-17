# Water-Info-Acquirer リファクタリング計画書 v1.0（案）

!!! note
    この文書は MkDocs 版の `docs/dev/refactoring-plan.md` にも掲載しています（内容は同等、整備はMkDocs側を優先）。

最終更新: 2025-10-10 JST  
対象リポジトリ: `Pckk-solvers/Water-Info-Acquirer`

---

## 1. この文書は何か（計画書の位置づけ）
- 既存機能を壊さずに保守性・拡張性・信頼性を高めるための **リファクタリング計画**。  
- **何を/どの順で/どんなルールで** 進めるか（ワークフロー）を明示。  
- 新機能追加ではなく、**内部構造の改善** が主目的。

---

## 2. 目的（ゴール）
- **重複ロジックの排除**（日/時データ取得・Excel出力の共通化）
- **責務分離**（GUI と 取得/整形/出力 の分離）
- **テスト容易性の確保**（最小ユニットテスト導入）
- **運用性の改善**（ログ/例外ハンドリング整備）
- **将来の拡張に耐えるパッケージ構造** への再編

---

## 3. 背景と現状（要約）
- 実装の集中: `src/main_datetime.py` に取得・整形・Excel出力・GUIが同居。  
- 重複: `src/datemode.py` 側にも類似の取得/出力処理。  
- 入口: `src/__main__.py`（`--single-sheet` 対応）。  
- 課題: 重複/結合度高/抽出処理の脆さ/ログ不足/テスト無し。

---

## 4. スコープ
- **対象**: データ取得/整形、Excel 出力、GUI 呼び出し部、エラー/ログ、パッケージ構成、最小テスト。  
- **非対象**: 新機能追加、外部仕様変更（CLI・Excel体裁）。

---

## 5. 方針（設計原則）
- **I/O とロジックの分離**（UIは薄く、処理はサービス層）
- **共通化/抽象化**（モード差分はテーブル駆動）
- **互換性維持**（出力の体裁・命名は原則維持）
- **段階的移行**（小PRの連続）
- **観測可能性**（ログ/例外の標準化）

---

## 6. 目標パッケージ構成（最終像）
```text
src/
  wia/
    __init__.py
    data_source.py     # 取得・整形（URL生成/観測所名抽出/日・時系列の共通パス）
    excel_writer.py    # Excel出力（全期間/年別/summary/チャート共通）
    gui.py             # WWRApp（UIは入力/表示のみ）
    errors.py          # EmptyExcelWarning など
    constants.py       # モード→KIND/単位/ラベル等のマップ
    types.py           # TypedDict / Protocol / 型定義
  __main__.py          # 既存CLI入口（--single-sheet継続）
```

---

## 7. ブランチ計画（優先順位 / ブランチ名 / 概要）

| P | ブランチ | 概要 |
|---|---|---|
| 1 | `refactor/p1-parse-fetch-core` | 取得コア統合・バグ修正・共通API化 |
| 2 | `refactor/p2-excel-writer` | Excel出力の共通レイヤ分離 |
| 3 | `refactor/p3-gui-separation` | GUI分離（UI薄く/サービス呼び出し） |
| 4 | `refactor/p4-error-logging` | logging導入・例外/終了コード整理 |
| 5 | `refactor/p5-package-layout` | 最終パッケージ構成へ移行 |
| 6 | `refactor/p6-tests` | pytest最小導入・回帰テスト |
| 7 | `refactor/p7-perf-session-concurrency` | requests.Session化・任意の並列 |
| 8 | `refactor/p8-style-typing-ci` | Ruff/Black/mypy・CI補強 |

---

## 8. タスク詳細（Doneの定義つき）

### P1 `refactor/p1-parse-fetch-core`
- 共通化: **モード→URL/ラベル/単位**のマップ、**観測所名抽出**、**日/時の系列化**。
- 修正: HTML抽出は `get_text(strip=True)`、数値化は `pd.to_numeric(errors="coerce")` を基準化。
- I/F例: `fetch_series(code, mode, period, granularity="hour|day") -> pd.DataFrame`（`datetime, value`）。
- **Done**: 既存日/時処理を新API置換しても出力Excelのデータ点が一致。

### P2 `refactor/p2-excel-writer`
- 共通化: **全期間/年別/summary** 出力と **散布図** 挿入（軸/凡例/サイズ共通）。
- I/F例: `write_timeseries_book(df, title_opts, single_sheet=False) -> Path`。
- **Done**: 既存Excelとシート名・列名・チャート有無が一致（体裁差は最小）。

### P3 `refactor/p3-gui-separation`
- `WWRApp` を `wia/gui.py` に移動。P1/P2 API 呼び出しに差し替え。
- **Done**: GUI操作で従来と同等のExcelが生成。`__main__.py` 入口は変更なし。

### P4 `refactor/p4-error-logging`
- `logging` 標準化（INFO/ERROR、ファイル+コンソール）。
- 例外方針: 業務例外（空データ）→ GUI: ポップ、CLI: 非ゼロ終了+メッセージ。想定外例外 → ログ+短文通知。
- **Done**: CLI/GUI 双方で一貫した挙動（ログ残る）。

### P5 `refactor/p5-package-layout`
- 目標ツリーへ移行、import ルート整理。PyInstaller は `src/__main__.py` ベース継続。
- **Done**: `python -m src.__main__` と 現行ワークフローのビルドが成功。

### P6 `refactor/p6-tests`
- `tests/` 追加：
  - URL 組立/期間分割のユニット。
  - HTMLスニペット→数値抽出。
  - Excel出力の **存在/シート/列/チャート定義** のスモーク。
- **Done**: 代表3テストがPRで緑。

### P7 `refactor/p7-perf-session-concurrency`（任意）
- `requests.Session()` で接続再利用、観測所ごと並列化（`concurrent.futures`）。
- **Done**: 代表ケースで体感改善、失敗率増なし。

### P8 `refactor/p8-style-typing-ci`（任意）
- Ruff/Black/mypy を dev 導入、主要APIに型。Actions に lint/test ワークフロー追加（リリース用は現行維持）。
- **Done**: PRで静的チェックが自動実行。

---

## 9. 開発ワークフロー（どう進めるか）
1) **ブランチ戦略**: P1→P2→P3→P4→P5 を小PRで連続。1PR=1目的。  
2) **コミット/PR**: Conventional Commits 推奨（例: `refactor: unify fetcher for hour/day`）。PR説明に 目的/変更点/影響/テスト/ロールバック を明記。  
3) **レビューDone基準**: 互換性維持・テスト緑・ログ/例外方針順守。  
4) **CI/CD**: 現行タグ駆動ビルドは継続。テスト導入後は PR 時テスト/リンタを別ワークフローで実行。

---

## 10. 品質メトリクス（軽量）
- **重複削減**: 取得/Excel 出力の実装箇所を1系統へ集約。  
- **テスト**: 最低3スモーク + 追加容易な構造。  
- **ログ**: 失敗時の原因追跡に必要十分な粒度（INFO/ERROR）。

---

## 11. リスクと対策（抜粋）
- **サイト構造変更/EUC-JP**: セレクタ一元管理・例外化。文字コードは明示指定＆テストにサンプルHTML保持。  
- **Excel差異**: シート名/列/チャート仕様を固定・共通化。  
- **PyInstaller差分**: 入口維持、最小スモークを毎PRで実施。

---

## 12. 互換性・移行方針
- CLIオプション・Excel出力の **外部仕様は維持**。  
- 既存利用者への影響最小。内部置換のみ。  
- 万一は **PR単位でリバート** 可能な小刻み運用。

---

## 13. 付録：今すぐ着手する順序（実務の一歩目）
1. `refactor/p1-parse-fetch-core` を作成。  
2. `wia/data_source.py` を新規作成（モード/観測所名/系列化）。  
3. 既存関数呼び出しを段階置換（差分比較で一致確認）。  
4. グリーン確認後、P2 `excel_writer.py` に進む。

