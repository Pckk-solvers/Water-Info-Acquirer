# Water-Info-Acquirer リファクタリング計画（案）

最終更新: 2025-10-10 JST  
対象リポジトリ: `Pckk-solvers/Water-Info-Acquirer`

!!! note
    これは「内部構造の改善」を目的とした計画書です。外部仕様（GUI/CLI、Excel体裁）の変更は最小限に留めます。

## 1. 目的（ゴール）

- **重複ロジックの排除**（日/時データ取得・Excel出力の共通化）
- **責務分離**（GUI と 取得/整形/出力 の分離）
- **テスト容易性の確保**（最小ユニットテスト導入）
- **運用性の改善**（ログ/例外ハンドリング整備）
- **将来の拡張に耐えるパッケージ構造** への再編

## 2. 背景と現状（要約）

- 実装の集中: `src/water_info/main_datetime.py` に取得・整形・Excel出力・GUIが同居。
- 重複: `src/water_info/datemode.py` 側にも類似の取得/出力処理。
- 入口: `main.py`（ランチャー）、`python -m water_info`（water_info単体）。
- 課題: 重複/結合度高/抽出処理の脆さ/ログ不足/テスト整備の余地。

## 3. スコープ

- **対象**: データ取得/整形、Excel 出力、GUI 呼び出し部、エラー/ログ、パッケージ構成、最小テスト。
- **非対象**: 新機能追加、外部仕様変更（CLI・Excel体裁）。

## 4. 目標パッケージ構成（最終像）

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
```

## 5. ブランチ計画（優先順位 / ブランチ名 / 概要）

| P | ブランチ | 概要 |
|---|---|---|
| 1 | `refactor/p1-parse-fetch-core` | 取得コア統合・共通API化 |
| 2 | `refactor/p2-excel-writer` | Excel出力の共通レイヤ分離 |
| 3 | `refactor/p3-gui-separation` | GUI分離（UI薄く/サービス呼び出し） |
| 4 | `refactor/p4-error-logging` | logging導入・例外/終了コード整理 |
| 5 | `refactor/p5-package-layout` | 最終パッケージ構成へ移行 |
| 6 | `refactor/p6-tests` | pytest最小導入・回帰テスト |
| 7 | `refactor/p7-perf-session-concurrency` | Session化・任意の並列 |
| 8 | `refactor/p8-style-typing-ci` | Ruff/Black/mypy・CI補強 |

## 6. 開発ワークフロー（どう進めるか）

1. **小PRの連続**（1PR=1目的）
2. **互換性維持**（出力やUIは原則維持）
3. **テスト/ログを並走**（破壊的変更を避ける）

## 7. 元資料

- `docs/water_info_acquirer_リファクタリング計画書_v_1.md`（初版）
