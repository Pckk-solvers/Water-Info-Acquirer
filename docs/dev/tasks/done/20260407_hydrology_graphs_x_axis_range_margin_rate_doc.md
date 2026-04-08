# X軸範囲マージン率（既定0）を文書へ追加

## 目的
- X軸データ範囲のマージン率を style JSON で設定できる仕様を明文化する。
- JSONスキーマ登録時の基準となるキー名・値制約・既定値を固定する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/render/plotter.py`
- `tests/hydrology_graphs/test_plotter.py`

## 実施内容
- 要件に `x_axis.range_margin_rate` を追加し、既定値を `0` と定義する。
- 設計に UI から style JSON へ同期し、preview/batch で共通適用する方針を追記する。
- style contract にキー、型、制約（`0以上`）を追記する。
- JSON Schema 設計の参照先を追加し、契約から辿れる状態にする。

## 完了条件
- 上記3文書でキー名・既定値・制約が矛盾なく記載されている。

## 確認方法
- 各文書で `x_axis.range_margin_rate` と `既定 0` の記載を目視確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 完了結果
- 要件に `x_axis.range_margin_rate`（既定 `0`、`0以上`）を追記した。
- 設計に `x_axis.range_margin_rate` の保持位置と描画反映ポイントを追記した。
- style contract にキー説明・制約・既定値を追記した。
- JSON Schema 設計文書 `style-json-schema-design.md` を追加した。
- フォーム項目に `X軸範囲マージン率` を追加した（`X軸角度` の直下）。
- 描画処理で `x_axis.range_margin_rate` を `ax.margins(x=...)` として反映するようにした。
- `tests/hydrology_graphs/test_plotter.py` を追加し、UI項目配置と描画反映を確認できるようにした。
