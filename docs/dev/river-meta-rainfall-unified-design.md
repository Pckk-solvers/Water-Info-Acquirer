# river_meta 雨量共通フォーマット化メモ

## 目的

`JMA` と `water_info` で取得元が異なる雨量データを、`river_meta` 側で共通フォーマットに正規化し、最後に出力整形する実装方針を整理する。

## 既存処理の確認結果

### JMA 側（`src/jma_rainfall_pipeline/`）

1. `WeatherDataController.fetch_and_export_data` が取得全体を統括する。  
   参照: `src/jma_rainfall_pipeline/controller/weather_data_controller.py:40`
2. `Fetcher.schedule_fetch` で期間分の HTML を取得する。  
   参照: `src/jma_rainfall_pipeline/fetcher/fetcher.py:136`
3. `parse_html(..., freq, obs_type)` で HTML を DataFrame 化する。  
   参照: `src/jma_rainfall_pipeline/parser/__init__.py:35`
4. 最後に `export_weather_data` で CSV/Excel を出力する。  
   参照: `src/jma_rainfall_pipeline/exporter/csv_exporter.py:302`

補足:
- JMA 側は `daily/hourly/10min` の粒度を持つ。  
  参照: `src/jma_rainfall_pipeline/fetcher/fetcher.py:33`
- 既に `WeatherDataRecord` という中間レコード型があり、`raw_data` も保持できる。  
  参照: `src/jma_rainfall_pipeline/domain/models.py:91`

### water_info 側（`src/water_info/`）

1. `fetch_hourly_dataframe_for_code` / `fetch_daily_dataframe_for_code` が取得と DataFrame 化を担当する。  
   参照: `src/water_info/service/flow_fetch.py:36`, `src/water_info/service/flow_fetch.py:105`
2. 雨量は `mode_type == "U"` で取得される。  
   参照: `src/water_info/service/flow_fetch.py:53`
3. 時刻データは `build_hourly_dataframe`、日データは `build_daily_dataframe` で整形される。  
   参照: `src/water_info/infra/dataframe_utils.py:9`, `src/water_info/infra/dataframe_utils.py:20`
4. 既存は `write_hourly_excel` / `write_daily_excel` で Excel 出力まで行う。  
   参照: `src/water_info/service/flow_write.py:40`, `src/water_info/service/flow_write.py:158`

補足:
- hourly で `display_dt = datetime + 1h` の表示用時刻を作っている。  
  参照: `src/water_info/infra/dataframe_utils.py:14`
- 取得値に `drop_last_each` など独自調整がある。  
  参照: `src/water_info/infra/fetching.py:34`

## 実現可能性の判断

結論: 実現可能。

根拠:
1. 両系統とも中間で `DataFrame` を生成しており、共通レコードへの写像がしやすい。
2. 既存コードが「取得」と「出力」をある程度分離しているため、Adapter 層を追加しやすい。
3. `river_meta` 側に既にサービス層・モデル層の土台があり、責務追加の受け皿がある。  
   参照: `src/river_meta/models.py:28`

主な注意点:
1. 観測所 ID 体系が異なる。  
   JMA は `prefecture_code + block_number (+ obs_type)`、water_info は `station code`。
2. 時刻解釈が異なる。  
   JMA 側には 24:00 相当処理、water_info 側には表示用 +1h がある。
3. 欠損値規約が異なる。  
   `None`, 空文字, `///`, `×` などを共通ルールで統一する必要がある。

## 推奨アーキテクチャ（river_meta）

### レイヤ分割

1. Source Adapter  
   `jma_rainfall_pipeline` / `water_info` から「取得直後 DataFrame」を受け取り、列名・時刻・単位を一次正規化する。
2. Normalizer  
   共通フォーマットへ最終写像し、欠損値・型・品質フラグを統一する。
3. Formatter  
   共通フォーマットから CSV/Excel/Parquet などへ最終整形する。

### 共通フォーマット案（最小）

- `source`: `"jma"` or `"water_info"`
- `station_key`: ソース内一意キー
- `station_name`: 観測所名
- `observed_at`: ISO8601（`Asia/Tokyo` 想定）
- `interval`: `"10min" | "1hour" | "1day"`
- `rainfall_mm`: 降雨量（数値）
- `quality`: `"normal" | "missing" | "estimated" | "unknown"`
- `raw`: 元列を保持する辞書（追跡用）

運用ルール:
1. 表示専用列（例: `display_dt`）は `observed_at` に採用しない。
2. 欠損は `None` に統一し、必要なら `quality` で理由を保持する。
3. 生データの列は `raw` に残してロスレス性を確保する。

## 実装フロー（段階導入）

1. Phase 1: 共通モデル定義  
   `river_meta` に `RainfallRecord`, `RainfallQuery`, `RainfallDataset` を追加する。
2. Phase 2: Adapter 実装  
   `jma_adapter` と `waterinfo_adapter` を実装し、既存 fetch/parse を呼び出して DataFrame を受ける。
3. Phase 3: Normalizer 実装  
   列名変換、時刻統一、欠損統一、quality 判定を集中実装する。
4. Phase 4: Formatter 実装  
   現状の Excel 出力仕様を壊さず、共通フォーマットからの出力を追加する。
5. Phase 5: CLI 追加  
   `river_meta` に `rainfall` サブコマンドを追加して source 選択実行できるようにする。
6. Phase 6: 既存 GUI 連携（必要なら）  
   既存 GUI フローは残したまま、内部実行経路のみ置換または併用する。

## テスト戦略

1. Adapter 単体テスト  
   HTML/レスポンス fixture から期待列が得られるかを検証する。
2. 共通フォーマット契約テスト  
   JMA / water_info の双方で同一スキーマが成立することを検証する。
3. 境界ケーステスト  
   月跨ぎ、24:00 相当、欠損混在、空データ、重複時刻を検証する。
4. 回帰テスト  
   既存出力（JMA CSV/Excel, water_info Excel）への影響がないことを確認する。

## 先に決めるべき仕様

1. `observed_at` を「観測時刻」で統一するか、「表示時刻」で統一するか。
2. 日データの `rainfall_mm` を「日合計」とみなすか、別フィールドを持つか。
3. 観測所メタ情報の共通キー（都道府県、河川系、座標）を最小どこまで持つか。

## 推奨する初手

1. まずは雨量のみ（`mode_type=U`）を対象に PoC を作る。
2. 出力は CSV のみに絞り、共通フォーマット整備を優先する。
3. PoC で時刻と欠損値の仕様を確定後、Excel 整形へ拡張する。
