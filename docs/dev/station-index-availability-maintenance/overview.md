# 観測所インデックス可用年メンテナンス Overview

## 背景

JMA / WaterInfo の観測所インデックス JSON は、現在は主に観測所メタデータの保持を目的としている。

- JMA: `src/river_meta/resources/jma_station_index.json`
- WaterInfo: `src/river_meta/resources/waterinfo_station_index.json`

一方で、データ取得系にはすでに「その観測所にどの年のデータがあるか」を扱う処理が存在する。

- JMA
  - `src/river_meta/rainfall/jma_availability.py`
  - `src/river_meta/services/rainfall.py`
- WaterInfo
  - `src/river_meta/service.py`
  - `src/river_meta/parser_availability.py`

今回やりたいことは、これらの可用年情報をインデックス JSON に取り込み、実行時に毎回サイトを見に行かなくても参照できるようにすること。

## この機能の位置づけ

この機能は、通常の利用機能ではなく、観測所インデックスを保守するためのメンテナンスツールとして設計する。

- 目的は「最新状態を保った JSON を定期更新できること」
- 実行主体は `scripts/` の更新スクリプト
- 利用主体は `src/river_meta/` の GUI / service / resolver

つまり責務分離としては次の形が自然。

- `scripts/`
  - 外部サイトへアクセスしてインデックスを再構築・補完・更新する
- `src/river_meta/`
  - 更新済み JSON を読み取り、通常機能で利用する

## 現行実装の事実

### 1. JMA の可用年判定

- `fetch_available_years_hourly()` が JMA の `index.php` を見て、存在年の集合を返す
- `run_rainfall_analyze()` では、その年集合で対象年を絞り込んでいる
- 年粒度の判定であり、厳密な終了日までは持たない

補足:

- JMA インデックスには PDF 由来の `start_date` がすでに入る
- これは「運用開始日」に近い属性情報であり、可用年リストの判定根拠にはしない
- JMA の可用年は、PDF ではなく URL ベースの availability 判定を正式採用する

### 2. WaterInfo の可用年取得

- `scrape_station()` が `SrchRainData.exe` の availability ページを読む
- `parse_availability_page()` が decade table から存在年リストを抽出する
- `StationReport` には `available_years_daily` / `available_years_hourly` がある

つまり WaterInfo 側では、旧実装上すでに「可用年リストを持つ」という発想がある。

### 3. Parquet 側の情報

- GUI の期間 CSV 出力などでは、Parquet スキャンから `available_years` を組み立てている
- ただしこれは「ローカルに取得済みの年」であり、「サイト上に存在する年」とは意味が異なる

今回の対象は後者、つまり外部サイト上の観測年の存在状況である。

## 採用する表現

データ存在期間は年粒度で管理する。

### 正本

- `available_years_hourly`
- `available_years_daily`（WaterInfo は正式対象）

範囲だけを持つ案は採用しない。理由は、欠年がある場合に情報を失うため。

例:

- `[1994, 1995, 1998]` を `1994-1998` にすると欠年が潰れる

そのため、JSON の正本は年リストのみとし、範囲表示が必要な場合は利用側で `min/max` から計算する。

## 目指す運用

最終的には次の運用を想定する。

1. `scripts/refresh_station_indexes.py` を定期実行する
2. JMA / WaterInfo のベース観測所情報を更新する
3. 可用年情報を再取得して JSON に反映する
4. GUI / service 側はその JSON を参照する

## 先に決めるべき論点

1. JMA の可用年を hourly のみにするか
- 現行処理は hourly 判定が中心
- 将来的に daily が必要ならキー拡張はできる

2. 実行モード
- `rebuild`: 全観測所を再計算
- `resume` / `update`: 既存 JSON を引き継いで更新

3. 利用側の参照優先順位
- 最終方針は JSON の可用年を唯一の参照元にする
- 移行期間のみ、必要ならオンライン判定を暫定フォールバックとして残す
