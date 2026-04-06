# Python/uvで実行

## 事前準備

- Python: `>=3.12`
- パッケージ管理: `uv`

## セットアップ

```powershell
uv sync
```

## 起動（ランチャー）

```powershell
uv run python main.py
```

## モジュール単体で起動

```powershell
# 国交省 水文データ取得
uv run python -m water_info

# 気象庁 雨量データ取得
uv run python -m jma_rainfall_pipeline
```

## テスト

```powershell
uv run pytest
```

## 時間契約の実データ検証

時間列の契約を再確認したい場合は、既知の検証コードを使うスクリプトを実行します。

```powershell
uv run python scripts/verify_time_contracts.py
```

- 出力先: `tmp/verify_time/`
- サマリ: `tmp/verify_time/report.json`
- 検証対象:
  - `water_info` 水位 `S`: `303051283310090`（高幡橋）
  - `water_info` 流量 `R`: `303031283301030`（栗橋）
  - `water_info` 雨量 `U`: `101031281101620`（定山渓ダム）
  - `JMA` daily / hourly / 10min: `13:47406:s1`（留萌）

## JMA の期間指定契約

- `jma-rainfall` の `--end YYYY-MM-DD` は、その日の終端ではなく「翌日 `00:00` の排他的上限」として扱います。
- そのため、`2026-03-01` から `2026-03-03` を指定すると、末尾の `24時` 相当は `2026-03-04 00:00:00` として保持されます。
- 新規出力では `23:59:59.999999` の疑似日末は生成しません。

## CLI出力形式（2026-04時点）

- `water-info fetch`
  - `--csv` は廃止。
  - 中間確認用の行指向JSONは `--ndjson` を使う。
  - 例:
    ```powershell
    uv run water-info fetch --code 303051283310090 --mode S --start 2025-01 --end 2025-01 --interval hourly --ndjson --parquet --no-excel
    ```
- `jma-rainfall fetch`
  - 既存の `--csv` / `--excel` / `--parquet` に加えて `--ndjson` を利用できる。
  - 例:
    ```powershell
    uv run jma-rainfall fetch --station 13:47406:s1 --start 2026-03-01 --end 2026-03-03 --interval hourly --ndjson --parquet --no-excel
    ```
