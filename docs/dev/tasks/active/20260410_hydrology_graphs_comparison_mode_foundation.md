# ハイドログラフ比較モード（2系列重ね合わせ）の基盤実装

## 目的
- ハイドログラフ（流量・水位）において、2つの観測所データを同一グラフ上に重ねて比較描画できる「比較モード」を実装する。
- スタイル調整タブにおいて、2本目の線の見た目を調整可能にし、プレビューで比較対象を切り替えられるようにする。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md` (要件)
- `docs/dev/architecture/hydrology_graphs.md` (設計)
- `src/hydrology_graphs/io/style_store.py` (スタイル)
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json` (スキーマ)
- `src/hydrology_graphs/ui/app.py` (UI・ダミーデータ)
- `src/hydrology_graphs/render/plotter.py` (描画)
- `src/hydrology_graphs/services/usecases.py` (サービス)

## 実施内容

### Phase 1: ドメイン・スタイル定義の拡張
- [ ] `style_schema_2_0.json` に `series2` オブジェクトを追加。
  - プロパティ: `enabled`, `color`, `width`, `style`, `use_secondary_y`
- [ ] `style_store.py` の `default_style` に `series2` の既定値（破線、淡色など）を追加。
- [ ] `domain/constants.py` または相当箇所に比較モード用の定数（既定の比較系列スタイルなど）を定義。

### Phase 2: スタイル調整タブ（UI）の拡張
- [ ] スタイル調整パレットに「比較系列(2本目)」行を追加。
  - ON/OFFトグル、色、太さ、線種、右軸使用フラグの編集をサポート。
- [ ] プレビュー領域に「メイン対象」と「比較対象」を選択する2つのドロップダウンを追加。
- [ ] 開発者モード (`_build_dev_dummy_catalog`) に比較テスト用のダミー観測所 `DEV002` を追加。
  - `DEV001` に対して位相がずれた、あるいは規模の異なる波形を生成。

### Phase 3: サービス層と描画ロジックの実装
- [ ] `usecases.py` のプレビュー/描画データ抽出処理を拡張。
  - 比較対象が指定されている場合、2系列分のデータを並列で抽出・整形する。
- [ ] `plotter.py` の描画ロジックを拡張。
  - 2系列目のデータがある場合、`series2` の設定に従って描画。
  - `use_secondary_y=true` の場合は `ax.twinx()` を用いて右軸にプロット。
- [ ] 複数系列表示時の凡例（Legend）自動生成機能を実装。

### Phase 4: 検証とテスト
- [ ] `test_style_store.py`: `series2` のスキーマバリデーションとデフォルト値のテストを追加。
- [ ] `test_plotter.py`: 複数系列描画および Y2 軸使用時の描画テストを追加。
- [ ] `test_services.py`: 比較モード時のデータ抽出ロジックのテストを追加。

## 完了条件
- スタイル調整タブで「比較系列(2本目)」の設定がプレビューに即座に反映される。
- 異なる観測所（DEV001, DEV002）を重ねて表示し、凡例で識別できる。
- Y2軸（右軸）への切り替えが正常に機能し、2つのスケールを同時に確認できる。

## 確認方法
- 開発者モードを起動し、ハイドログラフのスタイル調整タブで2系列重ね合わせを表示・操作する。
- 追加した単体テストがすべて通過することを確認する。
