# Water Info 後処理（GUI / CLI）

時間データ（_H系）と任意の日データ（_D系）のExcelを読み込み、日次集計・ランク付与・位況/流況算出・ピーク抽出などを行ってExcel/Parquetへ出力します。データ取得は既存のGUIで行い、後処理GUIには取得済みファイルを指定します。

## GUIで実行する

ランチャーで「水文統計（位況・流況）」を開き、対象種別、時間データ、任意の日データ、出力先を指定して「実行」を押します。`_WH`/`_QH`のファイル名は対象種別の初期値推定に利用されますが、最終的には画面で選択した対象種別が使われます。

## CLIで実行する場合

流況の場合は`--metric discharge`を追加します。位況の場合は`--metric water_level`を指定するか、省略時の既定値を使用します。

```powershell
uv run python -m water_info.postprocess `
  --metric discharge `
  --hour-file water_info/303031283303010_木原_2020年1月-2021年12月_QH.xlsx `
  --daily-file water_info/303031283303010_木原_2020年1月-2021年12月_QD.xlsx `
  --out-excel out/postprocess_result.xlsx `
  --out-parquet out/parquet_result
```

!!! tip "詳細仕様"
    後処理の要件は [開発 → Water Info 後処理](../dev/requirements/postprocess.md) にまとめています。
