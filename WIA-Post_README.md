# 実行時に設定ファイルを指定するとき

## 入力データの注意
全期間シートが存在しないと現時点ではエラーを出してしまいます。ご注意ください。


## 実行手順
cd で格納されているディレクトリへ移動する。（例）
cd C:\Users\yuuta.ochiai\Documents\GitHub\Water-Info-Acquirer


実行コマンド(例)
dist\WIA-Post.exe -h でヘルプを見ることができる。


設定ファイルを指定して実行する。（""dist\config.json""のように"" ""で囲まれるとエラー起こすので要注意）
dist\WIA-Post.exe --config dist\config.json


config.jsonの中身(スラッシュの向きに注意してください。)
{
  "hour_file": "C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/water_info/302111282214010_稲下_2000年1月-2010年12月_WH.xlsx",
  "daily_file": "",
  "out_excel": "water_info/postprocess/302111282214010_稲下_v2.xlsx",
  "out_parquet": null,
  "sheet_main": "main",
  "sheet_peaks": "peaks",
  "sheet_year_summary": "位況（順位正規化）",
  "sheet_year_summary_raw": "位況（順位固定）"
}
