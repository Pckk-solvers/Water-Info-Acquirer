# ハイエトグラフの2軸化・欠測可視化・軸調整（要件反映後の実装タスク）

## 目的
- ハイエトグラフを完成形に近づけるため、棒外枠、累積雨量の上下2段表示、欠測帯、Y軸調整を実装する。
- 追加仕様を style JSON / UIフォーム / 描画 / テストまで一貫して反映する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/render/plotter.py`
- `tests/hydrology_graphs/test_style_store.py`
- `tests/hydrology_graphs/test_plotter.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容（1回で実装・確認できる粒度に分解）

### Task 1: 契約キー確定と文書同期
- style contract / schema design / architecture に、ハイエト追加キーを定義する。
- キー候補:
  - `bar.width`（棒間隔の見え方調整）
  - `bar.edge_width`, `bar.edge_alpha`（棒外枠は黒固定）
  - `cumulative_line.enabled`, `cumulative_line.color`, `cumulative_line.width`, `cumulative_line.style`
  - `y_axis.max`, `y_axis.tick_step`（左軸）
  - `y2_axis.max`, `y2_axis.tick_step`（右軸・内部補助）
  - `grid.x_enabled`, `grid.y_enabled`（軸別グリッドON/OFF）
  - `missing_band.enabled`, `missing_band.color`, `missing_band.alpha`
  - `x_axis.data_trim_start_hours`, `x_axis.data_trim_end_hours`（表示前データ除外）
- 制約を明記:
  - 左右Y軸の下限は固定 `0`
  - 棒間隔の見え方は `bar.width` のみで調整し、時刻位置は変更しない
  - 棒外枠線の色は黒固定とする
  - `twinx` で単一プロット表示し、累積雨量側は自動上限優先とする
  - 刻みは指定優先（過密でも間引かない）
  - グリッド既定値は `x_enabled=false`, `y_enabled=true`
  - 欠測帯判定ルール（`quality=missing` または期待時刻欠落）
  - トリム単位は暫定 `0.5時間`（検証後に `1時間` へ変更判断）
  - トリムは描画前データに適用し、累積雨量はトリム後データで再計算する

### Task 2: style schema / style store 反映
- `style_schema_2_0.json` に追加キーの型・最小値・既定値を追加する。
- `style_store.py` の既定スタイル生成と正規化に追加キーを反映する。
- `None` や型不正入力時の検証エラー文言を既存ルールに合わせる。

### Task 3: フォーム反映（ハイエト対象のみ）
- スタイル調整フォームに追加項目を実装する。
- 対象が `hyetograph:*` 以外のときは項目を非表示または無効化する。
- ツールチップに「左/右Y軸下限は0固定」「刻みは指定優先」を明記する。
- ON/OFF と値調整を別行に分けず、1行構成へ統一する（左: チェックボックス / 右: 値入力）。
- グリッド表示は `X` と `Y` を個別チェックにし、同一行で編集できるようにする。
- 1行化対象を以下で固定実装する:
  - `累積雨量線` 行: 左=`cumulative_line.enabled` / 右=`width`,`style`,`color`
  - `欠測帯` 行: 左=`missing_band.enabled` / 右=`alpha`,`color`
  - `グリッド` 行: 左=`grid.x_enabled`,`grid.y_enabled` / 右=なし
  - `棒外枠` 行: 左=なし / 右=`bar.edge_width`,`bar.edge_alpha`
  - `Y軸(時間雨量)` 行: 左=なし / 右=`y_axis.max`,`y_axis.tick_step`
  - `棒幅` 行: 左=なし / 右=`bar.width`
- JSON直接編集との相互同期を維持する。

### Task 4: 描画反映（plotter）
- ハイエト描画時に `bar.width` を反映し、棒外枠線（黒固定、太さ・濃さ可変）を適用する。
- 累積雨量系列を計算し、右軸へ折れ線で描画する（ON時）。
- 欠測区間を連続区間として抽出し、グレー帯で描画する。
- 欠測区間は累積線を切断し、補間しない。
- 同一X軸で描画し、時刻位置のずれをなくす。
- 左右Y軸の下限を `0` 固定にし、累積側は自動上限で収める。
- `x_axis.data_trim_start_hours` / `x_axis.data_trim_end_hours` を適用し、先頭・末尾データを除外したうえで描画する。
- 累積雨量はトリム後データで再計算する。

### Task 5: テスト追加（部分テスト）
- schema/style_store テスト:
  - 追加キーの正常系・型不正系・既定値反映
- plotter テスト:
  - 2軸描画、棒外枠、欠測帯、Y軸設定反映
- UIテスト:
  - フォーム項目表示条件、1行配置、JSON同期の回帰
  - `grid.x_enabled=false`（既定）/`grid.y_enabled=true`（既定）の反映確認
- 実行コマンド（部分テスト）:
  - `uv run pytest -q tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_ui_support.py`

## 完了条件
- 要件/設計/契約/スキーマ/実装/テストが追加仕様に一致している。
- ハイエトグラフで2軸・欠測帯・棒外枠・Y軸設定が反映される。
- 上記部分テストが通る。

## 確認方法
- スタイル調整タブで `hyetograph` を選択し、追加フォーム項目が編集可能であることを確認する。
- 欠測を含むデータでプレビューし、欠測帯がグレー表示され、累積線が欠測区間で切れることを確認する。
- 左右Y軸の上限/刻み変更が描画に反映されることを確認する。
- 指定の部分テストを実行して通過を確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`（9章）
- 設計: `docs/dev/architecture/hydrology_graphs.md`
- 参照: `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- 参照: `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`

## 実装着手前レビュー
- このタスクは分解完了後、実装着手前にレビュー承認を得る。
- 承認観点:
  - 追加キー名と責務境界（UI / style schema / render）が妥当か
  - 欠測判定ルールが既存データ契約と矛盾しないか
  - テスト範囲が回帰リスクを十分にカバーしているか

---

## 更新メモ（2026-04-08, フォーム編集再開）

### 直近で進める対象
- Task 3（フォーム反映）を優先して再開する。
- 既存の分割方針に合わせ、UI変更は以下モジュール中心で実施する。
  - `src/hydrology_graphs/ui/style_form_builder.py`
  - `src/hydrology_graphs/ui/style_form_actions.py`
  - `src/hydrology_graphs/ui/style_palette_dialog.py`
  - `src/hydrology_graphs/ui/app.py`（coordinator 変更のみ）

### 実装粒度（1回で実装・確認する単位）
1. フォーム行レイアウトの統一（3列構成維持、1項目レンダリング共通化）
2. 日付境界線/基準線ラベル関連の入力幅と開始位置の統一
3. 設定ダイアログ再適用時の反映確認（閉じずに適用を含む）
4. 部分テスト/静的検証の実施
   - `uv run ruff check ...`
   - `uv run pyright ...`
   - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py`

### 追加メモ（2026-04-09, 細設定グルーピング）
- 細かいスタイル項目の増加に備え、フォーム分類を以下6グループで固定する。
  - 文字設定
  - 軸設定
  - グリッド・境界線
  - 系列・棒設定
  - スケール設定
  - データ範囲設定
- 実装方針:
  - 単一入力は通常行、複数入力は詳細設定行（設定ダイアログ）を使う。
  - スキーマキーは `graph_styles.<graph_key>` 配下を維持し、UI分類のみ追加する。
  - グループ見出しは表示必須にしない。ラベル名と配置（開始位置・列構成・余白）で分類を認知できる状態を優先する。
- 次の実装タスク（1回で完了できる粒度）:
  1. 上記6グループでフォーム順を固定化（見出し表示は任意、順序と配置を固定）
  2. palette化対象（色+線幅+線種）を行単位で追加
  3. 対応キーのツールチップ整備
  4. 部分テスト（UI表示順・同期）の更新

### 実装タスク分解（実装着手できる粒度）

#### Task A1: 行順定義の固定化（表示名なし運用）
- 目的: グループ見出しを表示しなくても、行順で分類が認知できる状態を作る。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
- 実施内容:
  - ハイエト向けの行順テーブルを定義し、行生成順を固定化する。
  - 既存項目の並び替えのみ行い、キー名や保存形式は変更しない。
- 完了条件:
  - プレビュー対象を切り替えても、同一グラフでは行順が不変。
- 確認方法:
  - 手動確認（スタイル調整タブで2回以上切替して行順一致を確認）。

#### Task A2: 共通レイアウト定数の一本化
- 目的: 項目入力の開始位置・列幅・余白の揺れを抑える。
- 対象ファイル:
  - `src/hydrology_graphs/ui/style_form_builder.py`
  - `src/hydrology_graphs/ui/tabs_style.py`
- 実施内容:
  - col1/col2 の開始位置と pad を共通定数で管理する。
  - ラジオボタン行も同じ開始位置ルールを使う。
- 完了条件:
  - 日付境界線、基準線ラベル、通常項目、ラジオボタンの開始位置が一致。
- 確認方法:
  - 手動確認（各行の左端が揃うこと）。

#### Task A3: 単一入力行と詳細設定行の判定ルール実装
- 目的: どの項目が通常行/詳細設定行かをコード上で明確化する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_form_builder.py`
- 実施内容:
  - 「1入力なら通常行」「色+線幅+線種など複数入力は詳細設定行」の判定を定義。
  - 判定結果に基づき row builder を分岐する。
- 完了条件:
  - 想定項目が意図した行形式で表示される。
- 確認方法:
  - 手動確認（対象行の表示形式を確認）。

#### Task A4: palette化対象の段階移行（第1段）
- 目的: 影響を限定しつつ複合設定をダイアログへ寄せる。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_palette_dialog.py`
- 実施内容:
  - `棒設定` と `系列設定` を詳細設定行として統一。
  - サマリ文字列の最大幅と省略記号表示を固定化。
- 完了条件:
  - 2行とも `設定...` で編集でき、サマリが更新される。
- 確認方法:
  - 手動確認（ダイアログ適用→サマリ更新→プレビュー更新）。

#### Task A5: palette化対象の段階移行（第2段）
- 目的: 境界線/補助線の複合項目を同じ運用に揃える。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_palette_dialog.py`
- 実施内容:
  - 日付境界線・基準線ラベルで複合入力が必要なものを詳細設定行へ移行。
  - 単一入力のものは通常行を維持。
- 完了条件:
  - 同種の複合入力が同じUI操作で編集できる。
- 確認方法:
  - 手動確認（各行の編集操作が統一されていること）。

#### Task A6: ツールチップ文言の日本語統一
- 目的: 行ラベルだけで意味が伝わらない項目の補足を統一する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_form_builder.py`
- 実施内容:
  - ON/OFF、オフセット、トリム、線種などの説明を日本語で統一。
  - 冗長な英語混在文言を除去。
- 完了条件:
  - 英語混在のツールチップが残らない。
- 確認方法:
  - `rg -n "tooltip"` で対象箇所を確認し、画面ホバーで文言確認。

#### Task A7: 部分更新回帰の確認
- 目的: フォーム編集時の全体再構築を抑制し、操作感を維持する。
- 対象ファイル:
  - `src/hydrology_graphs/ui/app.py`
  - `src/hydrology_graphs/ui/style_form_actions.py`
  - `src/hydrology_graphs/ui/style_palette_dialog.py`
- 実施内容:
  - 変更 path のみ更新されることを再確認し、必要なら差分更新対象を補強。
- 完了条件:
  - 設定適用時にスクロール位置・入力フォーカスが不必要に飛ばない。
- 確認方法:
  - 手動確認（スクロール中編集、連続適用、グラフ切替）。

#### Task A8: 検証と記録
- 目的: 本分解での実装結果を再現可能にする。
- 対象ファイル:
  - `tests/hydrology_graphs/test_ui_support.py`
  - `docs/dev/tasks/active/20260408_hydrology_graphs_hyetograph_dual_axis_missing_band.md`
- 実施内容:
  - UI表示順・同期の回帰テストを追加/更新。
  - 実施した確認結果をタスク文書に追記。
- 完了条件:
  - 最低限の部分検証が通る。
- 確認方法:
  - `uv run ruff check src/hydrology_graphs/ui tests/hydrology_graphs/test_ui_support.py`
  - `uv run pyright src/hydrology_graphs/ui`
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py`

### 実施結果（2026-04-09）
- A1 完了:
  - 行順固定ロジックを実装（`_STYLE_FIELD_PATH_ORDER` / `_ordered_style_fields`）。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A2 完了:
  - レイアウト定数を `style_form_builder.py` に集約し、`tabs_style.py` / `app.py` から参照。
  - 実装: `src/hydrology_graphs/ui/style_form_builder.py`, `src/hydrology_graphs/ui/tabs_style.py`, `src/hydrology_graphs/ui/app.py`
- A3 完了:
  - 通常行/詳細設定行の判定ルールを `_should_use_palette_row` として実装。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A4 完了:
  - `棒設定` / `系列設定` の詳細設定行運用を維持し、ツールチップを整備。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A5 完了（適用判定）:
  - 日付境界線・基準線ラベルは単一入力のため通常行を維持（判定ルールにより詳細設定行へ誤移行しないことを確認）。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A6 完了:
  - 追加/既存の複合設定行に日本語ツールチップを付与。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A7 完了:
  - 既存の部分更新実装を維持し、今回変更と両立することを確認。
  - 実装: `src/hydrology_graphs/ui/app.py`
- A8 完了:
  - 回帰テストを追加（順序固定・palette判定）。
  - 実装: `tests/hydrology_graphs/test_ui_support.py`

### 実行ログ（部分検証）
- `uv run ruff check src/hydrology_graphs/ui/app.py src/hydrology_graphs/ui/style_form_builder.py src/hydrology_graphs/ui/tabs_style.py tests/hydrology_graphs/test_ui_support.py` : pass
- `uv run pyright src/hydrology_graphs/ui/app.py src/hydrology_graphs/ui/style_form_builder.py src/hydrology_graphs/ui/tabs_style.py` : pass
- `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` : pass（18 passed）
