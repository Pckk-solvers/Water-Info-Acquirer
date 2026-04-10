# スタイル設定項目 拡張候補 提案資料

## 目的
- 水文グラフGUIのスタイル設定項目を段階的に増やすため、候補と優先順位を整理する。
- 「ラベル名と配置で伝わるUI」を前提に、実装負荷と効果のバランスを取る。

## スコープ
- 対象: スタイル調整タブ（フォーム/設定ダイアログ/プレビュー反映）
- 非対象: データ収集・スキャン・実行前検証ロジック

## 前提方針
- UI上でグループ見出しは必須にしない。
- 利用者には日本語ラベルのみを見せる。
- 複数値を持つ項目は「設定...」ダイアログで編集する。
- スキーマキーは `graph_styles.<graph_key>` 配下を維持する。

## 既存項目を含む運用方針（詳細）
- 既存項目も新規項目も、以下の同一基準でUI形式を決める。
  - 単一値: 通常入力行（Entry / Combobox / Checkbutton）
  - 複合値: 詳細設定行（設定ボタン + ダイアログ）
- 既存項目のUIを変更する場合は、見た目だけでなく操作手順の統一を優先する。
- 既存キー名は可能な限り維持し、互換性を崩す改名は行わない。

## 追加候補（提案）

### A. 文字設定
- タイトル文字サイズ
- X軸ラベル文字サイズ
- Y軸ラベル文字サイズ
- X目盛文字サイズ
- Y目盛文字サイズ
- 凡例文字サイズ
- 基準線ラベル文字サイズ
- 文字太さ（上記各項目）
- 文字色（上記各項目）

### B. 軸設定
- X軸ラベル位置オフセット
- Y軸ラベル位置オフセット
- X目盛ラベル位置（軸からの距離）
- Y目盛ラベル位置（軸からの距離）
- X軸角度（既存の明示化）
- X軸範囲マージン率（既存の明示化）

### C. グリッド・境界線
- グリッド主線の色
- グリッド主線の太さ
- グリッド主線の線種
- 日付境界線の色
- 日付境界線の太さ
- 日付境界線の線種
- 日付境界線オフセット（既存の明示化）

### D. 系列・棒設定
- 系列色（既存）
- 系列太さ（既存）
- 系列線種（既存）
- マーカー形状
- マーカーサイズ
- 棒色（既存）
- 棒幅（既存）
- 棒外枠太さ（既存）
- 棒外枠濃さ（既存）

### E. スケール設定
- Y軸上限（既存）
- Y軸刻み（既存）
- Y軸数値形式（既存）
- 小数桁数
- 単位接尾辞（例: mm, m3/s）
- ハイエトグラフ2軸個別設定（左軸=時間雨量、右軸=累積雨量）
  - 左軸上限 / 左軸刻み / 左軸数値形式
  - 右軸上限 / 右軸刻み / 右軸数値形式
  - 左右軸連動ON/OFF（必要時）

### F. データ範囲設定
- 表示データ範囲トリム有効（既存）
- 先頭トリム時間（既存）
- 末尾トリム時間（既存）
- トリム後再計算ルール（説明強化）

## 優先度（実装順提案）

### 優先度1（効果が高く、実装リスクが低い）
1. 文字サイズの個別化（タイトル/軸ラベル/目盛/凡例）
2. 軸ラベル位置オフセット（X/Y）
3. グリッド線の色・太さ・線種

### 優先度2（効果が高いが、UI設計の整理が必要）
1. 系列マーカー（形状/サイズ）
2. 日付境界線の線スタイル一式（色/太さ/線種）
3. ハイエト2軸の個別設定（左右Y軸）
4. Y軸数値の小数桁数・単位接尾辞

### 優先度3（後段で十分）
1. 文字太さ・文字色の全項目個別化
2. 詳細な目盛位置調整（tick padの細分化）
3. 追加の凡例表示制御

## UI形式の割当（既存 + 追加候補）

### 1. 詳細設定行（設定ボタン）にする項目
- 既存:
  - 系列設定（色 / 太さ / 線種）
  - 棒設定（棒色 / 棒幅 / 外枠太さ / 外枠濃さ）
  - 累積雨量線（色 / 太さ / 線種）
  - 欠測帯（色 / 濃さ）
- 追加候補:
  - グリッド線（色 / 太さ / 線種）
  - 日付境界線（色 / 太さ / 線種 / オフセット）
  - フォント詳細（対象別のサイズ / 太さ / 色）

### 2. 通常入力行のまま維持する項目
- 既存:
  - 図幅 / 図高 / DPI
  - フォント（共通）
  - 基本フォントサイズ
  - タイトルテンプレート
  - X軸ラベル / Y軸ラベル
  - X軸角度
  - X軸範囲マージン率
  - 表示データ範囲トリム（有効 / 先頭 / 末尾）
  - Y軸設定（上限 / 刻み / 数値形式）
  - 凡例表示、Y軸反転
- 追加候補:
  - 軸ラベル位置オフセット（X / Y）
  - 目盛ラベル位置（X / Y）
  - 小数桁数

### 3. 境界項目（要検証）
- 日付境界線:
  - 最小構成（表示ON/OFF + オフセットのみ）なら通常入力行でも可
  - 線スタイルまで扱う場合は詳細設定行へ移行
- 基準線ラベル:
  - オフセットのみなら通常入力行
  - 色・フォント・太さを追加する場合は詳細設定行
- ハイエト2軸:
  - 左右軸それぞれを通常入力行で持つと行数が増えるため、
    「Y軸設定（左）」「Y軸設定（右）」を詳細設定行化する案を優先する。

## 最小リリース案（第1弾）
- タイトル文字サイズ
- X軸ラベル文字サイズ
- Y軸ラベル文字サイズ
- X目盛文字サイズ
- Y目盛文字サイズ
- 軸ラベル位置オフセット（X/Y）
- グリッド線の色・太さ・線種

## 実装ステップ（既存項目を含めた段階移行）
1. 既存の詳細設定行（系列/棒/累積/欠測）を現行維持し、ラベルとツールチップだけ統一する。
2. 既存の通常入力行を固定順で再配置し、開始位置・余白を統一する。
3. 追加候補のうち、複合値を持つものから詳細設定行へ追加する。
4. 単一値候補を通常入力行へ追加する。
5. 各段階で部分更新回帰（全体再構築しないこと）を確認する。

## 廃止方針（合意事項）

以下は UI から段階的に削除する。

1. 重複単独行（系列/棒）
- 対象: `series_color` / `series_width` / `series_style` の単独入力行
- 対象: `bar_color` / `bar.width` の単独入力行
- 理由: 既存の「系列設定」「棒設定」詳細設定行と重複するため

2. 基本フォントサイズ
- 対象: `font_size` の通常入力行
- 理由: 個別フォントサイズ導入後は競合しやすく、意図が不明瞭になるため

3. グリッド全体ON/OFF
- 対象: `grid.enabled` の通常入力行/トグル
- 理由: `grid.x_enabled` / `grid.y_enabled` で十分に制御可能であり二重管理になるため

### 互換運用
- 既存JSONの読込時は上記キーを受け入れる（互換維持）。
- UIでは非表示にし、新規編集では対応する個別設定へ誘導する。
- 保存時の扱いは別タスクで決定する（保持/正規化のどちらかを明示）。

## 確認観点
- 既存スタイルJSONとの互換性を壊さないこと
- 設定反映時にフォームが全体再構築されないこと
- 日本語ラベルだけで操作意図が伝わること
- プレビューに即時反映されること

## 関連文書
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- `docs/dev/tasks/active/20260408_hydrology_graphs_missing_data_policy_shift.md`

## 実装タスク分解（採用後 / 実装可能粒度）

### S1: キー契約の追記（2軸・フォント個別・グリッド線詳細）
- 目的: 実装前に追加キーを確定する。
- 対象ファイル:
  - `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
  - `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- 実施内容:
  - ハイエト2軸個別キー（左/右）を明記
  - フォント個別サイズキーを明記
  - グリッド線の色/太さ/線種キーを明記
- 完了条件:
  - 追加キー名と用途がドキュメントで一意に定義される。
- 確認方法:
  - 文書差分レビュー（名称衝突なし）。

### S2: スキーマ更新（追加キー + 廃止方針反映）
- 目的: JSON検証で新旧キー運用を制御可能にする。
- 対象ファイル:
  - `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- 実施内容:
  - 追加キーを schema に追加
  - `font_size` / `grid.enabled` は互換読込対象として残し、UI編集対象から外す方針をコメント反映
- 完了条件:
  - スキーマ検証で新キーが通り、型不正は検出される。
- 確認方法:
  - `uv run pytest -q tests/hydrology_graphs/test_style_store.py`

### S3: style_store 正規化実装
- 目的: 既存JSON互換を保ったまま内部値へ正規化する。
- 対象ファイル:
  - `src/hydrology_graphs/io/style_store.py`
  - `tests/hydrology_graphs/test_style_store.py`
- 実施内容:
  - 旧キー（`font_size`, `grid.enabled`）を読込時にフォールバック処理
  - 新キー未指定時の既定値補完
- 完了条件:
  - 既存JSON読込でエラーにならず、新キー運用へ移行できる。
- 確認方法:
  - style_store テスト追加/更新。

### S4: 既存重複行のUI削除（系列/棒単独行）
- 目的: 重複UIを除去し、詳細設定行へ一本化する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
- 実施内容:
  - `series_*` / `bar_*` 単独入力行を非表示化
  - `系列設定` / `棒設定` への導線のみ残す
- 完了条件:
  - 画面上に重複項目が表示されない。
- 確認方法:
  - 手動確認（スタイルタブ）。

### S5: 基本フォントサイズ行のUI削除
- 目的: 個別フォント設定導入に向けて競合項目をなくす。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
- 実施内容:
  - `font_size` の通常入力行を非表示化
- 完了条件:
  - 基本フォントサイズがUIに出ない。
- 確認方法:
  - 手動確認（スタイルタブ）。

### S6: グリッド全体ON/OFF行のUI削除
- 目的: `x/y` 個別制御へ統一する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
- 実施内容:
  - `grid.enabled` 行/トグルをUIから除去
  - `grid.x_enabled` / `grid.y_enabled` を編集導線として維持
- 完了条件:
  - グリッド全体ON/OFFがUIに出ない。
- 確認方法:
  - 手動確認（スタイルタブ）。

### S7: フォント個別サイズのUI追加（第1弾）
- 目的: 最小リリース対象を実装する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_palette_dialog.py`（必要時）
- 実施内容:
  - タイトル / X軸ラベル / Y軸ラベル / X目盛 / Y目盛 の文字サイズ入力を追加
- 完了条件:
  - 該当項目が編集でき、プレビューに反映される。
- 確認方法:
  - 手動確認 + 既存プレビュー回帰。

### S8: 軸ラベル位置オフセットUI追加（X/Y）
- 目的: 軸ラベル位置調整を可能にする。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/render/plotter.py`
  - `tests/hydrology_graphs/test_plotter.py`
- 実施内容:
  - X/Y軸ラベル位置オフセット項目を追加
  - 描画側で反映
- 完了条件:
  - オフセット値変更で描画位置が変わる。
- 確認方法:
  - plotter テスト + 手動確認。

### S9: グリッド線スタイル詳細追加（色/太さ/線種）
- 目的: グリッド調整の実用性を上げる。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/render/plotter.py`
  - `tests/hydrology_graphs/test_plotter.py`
- 実施内容:
  - グリッド線スタイルを詳細設定行へ追加
  - 描画反映を追加
- 完了条件:
  - グリッド設定の変更がプレビューに反映される。
- 確認方法:
  - plotter テスト + 手動確認。

### S10: ハイエト2軸個別設定（左/右Y軸）
- 目的: ハイエト2軸を独立調整可能にする。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/render/plotter.py`
  - `tests/hydrology_graphs/test_plotter.py`
- 実施内容:
  - 左右Y軸の上限/刻み/数値形式を個別設定
  - UIは「Y軸設定（左）」「Y軸設定（右）」詳細設定行で提供
- 完了条件:
  - 左右Y軸が独立して反映される。
- 確認方法:
  - plotter テスト + 手動確認。

### S11: 回帰検証と文書同期
- 目的: 変更を安定化して完了条件を満たす。
- 対象ファイル:
  - `tests/hydrology_graphs/test_ui_support.py`
  - `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
  - `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- 実施内容:
  - UI表示項目の回帰テスト追加
  - 仕様文書を最終状態へ同期
- 完了条件:
  - 部分検証が通り、文書と実装が一致する。
- 確認方法:
  - `uv run ruff check src/hydrology_graphs/ui src/hydrology_graphs/render src/hydrology_graphs/io tests/hydrology_graphs`
  - `uv run pyright src/hydrology_graphs/ui src/hydrology_graphs/render src/hydrology_graphs/io`
  - `uv run pytest -q tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_ui_support.py`

## 実施結果（2026-04-09）
- S1〜S11 実装完了。
- 反映要点:
  - 文字サイズ個別キー、軸ラベル/目盛位置、グリッド線詳細、ハイエト右軸個別設定を実装。
  - 重複単独行（系列/棒）・基本フォントサイズ・グリッド全体ON/OFFのUI表示を廃止。
  - 既存JSON互換（`font_size` / `grid.enabled` 読込）を維持。
- 検証結果:
  - `uv run ruff check src/hydrology_graphs/io/style_store.py src/hydrology_graphs/render/plotter.py src/hydrology_graphs/ui/app.py tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_style_form_actions.py` : pass
  - `uv run pyright src/hydrology_graphs/io/style_store.py src/hydrology_graphs/render/plotter.py src/hydrology_graphs/ui/app.py tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_style_form_actions.py` : pass
  - `uv run pytest -q tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_style_form_actions.py tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` : pass（52 passed）
