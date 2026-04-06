# Water Info 時刻・値対応付けの整理

## 目的
- `water_info` の時間データを取得時点から `datetime` ベースで保持し、時刻列と値列のずれを解消する。
- Excel 出力と Parquet 出力で同じ時刻解釈を使えるようにする。

## 対象ファイル
- `docs/dev/requirements/water_info.md`
- `docs/dev/architecture/water_info.md`
- `src/water_info/infra/scrape_values.py`
- `src/water_info/service/flow_fetch.py`
- `src/water_info/infra/dataframe_utils.py`
- `src/water_info/entry.py`
- `src/water_info/service/flow_write.py`

## 実施内容
- HTML の日付行 + 24 時間列で時刻と値を対応付ける。
- source 側の中間データを `datetime` ベースに寄せる。
- 24時相当を翌日 `00:00:00` として統一する。
- Excel 出力の時刻表示と Parquet 保存の時刻解釈を揃える。
- CLI で `JSONL` / `Excel` / `Parquet` を同一条件で出力し、実HTMLと突き合わせる。

## 完了条件
- 時刻列とデータ列のずれが発生しない。
- source 側で `datetime` を確認できる。
- `period_end_at` / `observed_at` の意味が出力まで一貫している。

## 確認方法
- 代表的な hourly / daily データで、行ごとの時刻と値が一致していることを確認する。
- Excel 出力の時刻列と Parquet の `period_end_at` が一致することを確認する。
- `water_info` CLI を実行し、`*.xlsx` / `*.parquet` / `*.ndjson` と標準出力の JSON を確認する。
- 実HTML の先頭行と、CLI 出力の先頭行が一致するかを比較する。

## 結果
- hourly 取得フローで、HTML の行構造に合わせた抽出と旧形式フォールバックを実装した。
- `extract_hourly_readings()` を、日付行 + 24 時間列の実HTMLに合わせて解釈するようにした。
- `water_info` CLI の実行結果を `JSONL` / `Excel` / `Parquet` で出力し、実HTMLと比較できるようにした。
- `uv run pytest -q tests/water_info` を実行し、`40 passed` を確認した。
- `303031283301020` の 2024/01 取得について、実HTML の 12 月ページ + 1 月ページと CLI 出力を比較し、先頭行から末尾行まで一致することを確認した。

## 残課題
- 取得対象を変えたときの HTML 行構造の揺れに備えて、別フォーマットが出た場合のフォールバックは維持する。
- 必要なら、比較用の実行例を別途 `docs/dev/reference/` に追加する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/water_info.md`
- 設計: `docs/dev/architecture/water_info.md`
