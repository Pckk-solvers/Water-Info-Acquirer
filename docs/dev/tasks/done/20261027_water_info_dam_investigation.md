# 長安口ダムの取得可能性調査

## 目的

観測所 `608061288803010` の取得可否を確認し、GUI対応で整理すべき項目と制約を提案する。

## 対象ファイル

- `docs/dev/reference/water_info-dam-investigation.md`
- `docs/dev/tasks/active/20261027_water_info_dam_gui.md`
- `outputs/water_info/investigation/608061288803010/`（ローカル調査成果物）

## 実施内容

公式の検索ページ・JavaScript・データページ・ダウンロード・iframeを確認し、現行GUIと取得・出力層との差分を調べた。

## 完了条件・確認方法

- 過去の時間値を取得し、日時・5項目を表示表と照合する。
- 期間制限・時刻・属性・単位とGUI構成案を記録する。
- アプリ実装は行わず、未確定事項と実装計画を残す。

## 関連要件・関連設計

- `docs/dev/requirements/water_info.md`
- `docs/dev/domain/time-semantics.md`
- `docs/dev/architecture/water_info.md`

## 自己レビュー結果

- 調査範囲とユーザーの指示の整合: OK。
- 観測所情報ページの403をデータ取得不可と誤認しない: OK、ダム専用経路で実測。
- 調査と実装の区別: OK、アプリコードは変更しない。

## 結果・残課題

- 2026-10-27完了。過去3期間840行を取得し、日時・通常数値・閉局表示を照合した。
- 2024年1月744行の調査Excel、3期間のCSV、原データと確認JSONを保存した。
- `#` 属性8項目値がHTMLの閉局に対応することを確認した。
- 後続作業は [ダムGUI対応・全期間取得](20261027_water_info_dam_gui.md) で完了した。時間意味を断定した独自集計は対象外とした。
