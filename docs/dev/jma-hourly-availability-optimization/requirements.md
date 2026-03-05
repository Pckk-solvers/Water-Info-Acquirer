## 事実確認（実装ベース）
1. JMA 現行取得
- `src/river_meta/services/rainfall.py` は JMA を月単位で取得し、月ごとに `parquet_exists` を判定している。
- JMA 側に「観測所ごとの存在年」を事前取得する処理は未実装。

2. WaterInfo 現行取得
- WaterInfo には既存で `river_meta.service.scrape_station()` に存在年抽出処理がある。
- `src/river_meta/parser_availability.py` は `SrchRainData.exe` の decade テーブルを解析し、`available_years_*` を作る。
- これは river.go.jp（WaterInfo系）向けロジックであり、JMA の `etrn/index.php` とは別仕様。

3. 既存再利用の境界
- WaterInfo 側は既存処理を再利用可能。
- JMA 側は新規実装が必要（JMA専用の年一覧取得）。

## WaterInfo 既存処理の詳細（再利用対象）
1. 観測所IDの収集（検索一覧）
- 実装: `src/river_meta/station_ids/core.py`
- 入口:
  - `fetch_master_options()` : 都道府県・観測項目マスタ取得
  - `collect_station_ids()` : `SrchSite.exe` を `PAGE` 付きで巡回して ID 一覧収集
- URL:
  - `SRCHSITE_URL = https://www1.river.go.jp/cgi-bin/SrchSite.exe`
- 主要パラメータ:
  - `KEN`（都道府県）, `KOMOKU`（観測項目）, `SUIKEI`, `KASEN`, `CITY`, `NAME`

2. 観測所メタデータ取得
- 実装: `scripts/build_waterinfo_station_index.py`
- 入口:
  - `fetch_site_info()` : `SiteInfo.exe?ID=...` から詳細テーブル取得
  - `parse_metadata()` : 観測所名/ふりがな/水系/河川/所在地/緯度経度を正規化
- 生成物:
  - `src/river_meta/resources/waterinfo_station_index.json` の `by_station_id`

3. 存在年（available years）取得
- 実装:
  - `src/river_meta/service.py` の `scrape_station()`
  - `src/river_meta/parser_availability.py` の `parse_availability_page()`
- URL:
  - `SrchRainData.exe?ID={station_id}&KIND={kind}&PAGE={page}`
- 出力:
  - `available_years_daily`
  - `available_years_hourly`
- 補足:
  - decade テーブル（`xxx*`）と「有」セルから年を復元するロジック

4. 現在の rainfall 実行フローでの WaterInfo 利用点
- 観測所コード解決:
  - `src/river_meta/rainfall/waterinfo_station_index.py`
  - `resolve_waterinfo_station_codes_from_prefectures()`
- 年データ取得:
  - `src/river_meta/services/rainfall.py` の `_fetch_waterinfo_year()`
  - 年単位で `parquet_exists(output_dir, \"water_info\", ...)` を確認してから取得

## 目的
- JMA `hourly` 取得時に、観測所ごとの利用可能年を事前取得して無駄リクエストを削減する。
- WaterInfo は既存の存在年抽出ロジックを維持・活用し、重複実装を避ける。
- WaterInfo は今回はリクエスト最適化を行わず、Parquet 保存条件のみ改善する。

## スコープ
- 対象A（新規）: JMA 用の存在年取得（`etrn/index.php` ベース）
- 対象B（適用）: JMA 取得フローへの年フィルタ適用
- 対象C（適用）: WaterInfo の Parquet 保存ガード（有効値ゼロ時は保存しない）
- 非対象: WaterInfo の年存在判定適用、WaterInfo URL仕様の全面見直し、年一覧キャッシュ導入

## 適用経路
- 今回の JMA 年判定適用先は `run_rainfall_analyze()` のみとする（GUI のデータ取得経路）。
- `run_rainfall_collect()` は今回対象外とし、既存挙動を維持する。

## 機能要件
1. JMA: 存在年取得（hourly）
- JMA の `index.php` から観測所ごとの年候補を取得する。
- 今回対象は `hourly` 用のみ（JMA内部の該当頻度）。
- JMA判定は JMA 専用実装とし、`parser_availability.py` を流用しない。
- 判定に PDF の `start_date` は利用しない（JMA HTML 事実値のみで判定）。

1-1. JMA: URL組み立て仕様（新規）
- 年判定ページURL（新規機能）:
  - `https://www.data.jma.go.jp/stats/etrn/index.php`
  - クエリ: `prec_no`, `block_no`, `year`, `month`, `day`, `view`
- 既定パラメータ方針:
  - `prec_no` : 2桁文字列（例: `62`）
  - `block_no`: 観測所コード（例: `0604`）
  - `year/month/day/view` は空文字で初回照会（ページ側の候補年表示を利用）
- サンプル:
  - `.../index.php?prec_no=62&block_no=0604&year=&month=&day=&view=`
- 実装ルール:
  - URL組み立ては専用関数に集約し、文字列連結を分散させない。
  - `prec_no` / `block_no` の正規化（trim, `prec_no` の2桁化）を関数内で実施する。

1-2. JMA: 既存データ取得URLとの責務分離
- 年判定用URLと実データ取得URLを混同しない。
- 実データ取得は既存どおり:
  - `.../stats/etrn/view/{freq}_{station_type}.php?...`
- 本要件で追加するのは「年判定用 index.php」のみ。

2. JMA: 取得対象年の絞り込み
- ユーザー指定年と JMA 判定年の積集合を取り、対象年を絞る。
- ただし「年判定が成功した観測所」に限って積集合を採用する。
- 積集合が空なら当該観測所の取得はスキップし、ログ出力する。

3. JMA: Parquet 判定との併用
- 年フィルタ後も既存の月次 `parquet_exists` 判定は維持する。
- フロー: `JMA存在年判定 -> 年フィルタ -> 月次Parquet判定 -> リクエスト`

4. JMA: フォールバック
- JMA 年判定に失敗した観測所は従来方式（月次ループのみ）で継続。
- 観測所単位で失敗を隔離し、全体停止しない。

4-1. JMA: 判定結果の分類（実装判断基準）
- `success_with_years`: 指定観測所（`prec_no`/`block_no`）に一致する年リンクを1件以上抽出できた。
- `success_empty`: 指定観測所の文脈は保持されているが、年リンクが0件だった。
- `indeterminate`: 通信失敗、HTML解析失敗、または指定観測所文脈が確認できない。
- `success_*` のみ年フィルタを適用し、`indeterminate` は従来方式へフォールバックする。

5. WaterInfo: 方針明確化
- WaterInfo は既存の `scrape_station()` / `parse_availability_page()` を再利用対象とする。
- JMA対応のために WaterInfo 既存ロジックを改変しない。

6. WaterInfo: Parquet 保存条件の改善
- 現行実装は `part.records` が1件以上あれば Parquet を保存する。
- 追加要件として、`rainfall_mm is not None` の有効レコード件数を確認する。
- 有効レコード件数が 0 件の場合:
  - Parquet は保存しない
  - ログに「有効値なしのため保存スキップ」を出す
- 有効レコードが 1 件以上ある場合のみ Parquet 保存する。

7. WaterInfo: 今回の変更境界
- `flow_fetch.py` の URL 組み立て方式（`BGNDATE`/`ENDDATE` の作り方）は今回変更しない。
- 今回は「不要Parquetの抑止」に限定して改善する。

## UX要件
1. 取得前可視化
- ログに「観測所ごとの 指定年数 -> 判定後年数」を出す。
- 全体で何年分削減できたかを1行で表示する。

2. フォールバック可視化
- 判定失敗観測所は「従来モードで継続」を明示する。
- `success_empty` は失敗扱いにせず「データ存在年なし」として明示する。

## 受け入れ条件
1. JMA で存在年取得処理が動作し、年フィルタが適用される。
2. JMA 年判定失敗時に従来フローへフォールバックする。
3. 既存の月次 Parquet スキップは維持される。
4. WaterInfo は有効値 0 件の年データで Parquet を作成しない。
5. WaterInfo 年単位の既存 Parquet スキップ判定は維持される。
6. ログで削減効果（JMA）と WaterInfo 保存スキップ理由が確認できる。
7. JMA 年判定URLが専用ビルダー経由で生成される（`index.php` 用）。
8. JMA 判定の適用範囲が `run_rainfall_analyze()` に限定されている。
9. `indeterminate` は必ず従来フローへフォールバックし、年フィルタを適用しない。
