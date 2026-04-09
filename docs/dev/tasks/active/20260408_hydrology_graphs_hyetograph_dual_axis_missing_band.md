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
