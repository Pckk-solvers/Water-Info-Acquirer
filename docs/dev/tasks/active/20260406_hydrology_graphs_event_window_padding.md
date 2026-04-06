# イベント系窓の終端+1時間対応

## 目的
- Hydrology Graphs のイベント系で、24時相当の記録を取りこぼさないように基準日窓の終端側へ1時間の余白を持たせる。
- 描画設定からこの補正を選択できるようにする前提を、要件・構成と整合させる。
- `water_info` の datetime 正規化完了を前提に、描画側の窓補正だけを扱う。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/adr/20260406_event_window_terminal_padding.md`
- `src/hydrology_graphs/domain/logic.py`
- `src/hydrology_graphs/services/usecases.py`
- `src/hydrology_graphs/ui/*`

## 実施内容
- イベント系の logical window と capture window を分ける。
- 窓補正設定の値を precheck / preview / batch に流す。
- 3日 / 5日 のイベント系判定と描画で同じ窓定義を使う。
- 24時相当が窓端にあるケースの取りこぼしを防ぐ。
- 入力データは `period_end_at` / `observed_at` が揃った parquet を前提とする。

## 完了条件
- 基準日窓の終端側に1時間の余白を持たせる動作が実装されている。
- 描画設定で補正の有無を切り替えられる。
- precheck / preview / batch で結果が一致する。
- 関連文書が実装と一致している。

## 確認方法
- 24時相当を翌日 `00:00:00` として持つデータで、イベント系が欠損判定にならないことを確認する。
- 補正あり / なし で precheck の結果差分を確認する。
- プレビューとバッチで同じ窓定義が使われることを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`
- 判断記録: `docs/dev/adr/20260406_event_window_terminal_padding.md`
