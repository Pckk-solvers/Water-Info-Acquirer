# Water Info 後処理（CLI）

時間データ（_H系）と日データ（_D系）のExcelを読み込み、日次集計・ランク付与・位況算出・ピーク抽出などを行ってExcel/Parquetへ出力します。

## 実行例

```powershell
uv run python -m water_info.postprocess `
  --hour-file water_info/303031283303010_木原_2020年1月-2021年12月_WH.xlsx `
  --daily-file water_info/303031283303010_木原_2020年1月-2021年12月_WD.xlsx `
  --out-excel out/postprocess_result.xlsx `
  --out-parquet out/parquet_result
```

!!! tip "詳細仕様"
    後処理の仕様は [開発 → Water Info 後処理（設計資料）](../dev/postprocess/specification.md) にまとめています。

