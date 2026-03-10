# 観測所インデックス可用年メンテナンス 詳細設計

## 1. 目的

観測所インデックス JSON を、単なる観測所名簿ではなく「可用年つきの保守対象マスタ」として扱えるようにする。

この設計の中心は以下。

- 年リストを正本とする
- 更新責務を `scripts/` に寄せる
- 利用側は JSON を読む
- `refresh_station_indexes.py` を定期更新入口にする

## 2. 変更対象

### scripts

- `scripts/build_waterinfo_station_index.py`
- `scripts/update_jma_station_index.py`
- `scripts/refresh_station_indexes.py`
- `scripts/update_waterinfo_station_availability.py`
- `scripts/update_jma_station_availability.py`

### src

- `src/river_meta/rainfall/jma_availability.py`
- `src/river_meta/service.py`
- `src/river_meta/parser_availability.py`
- JMA / WaterInfo インデックス読込側

## 3. 役割分担

### 3.1 更新層

更新層は `scripts/` とし、役割は以下。

- ベース観測所情報の生成
- 可用年の取得
- JSON への反映
- 途中保存
- ログ出力

### 3.2 利用層

利用層は `src/river_meta/` とし、役割は以下。

- JSON の読込
- 観測所解決
- 実行時の対象年判定
- GUI 表示

## 4. JSON スキーマ案

### 4.1 JMA

既存観測所オブジェクトに以下を追加する。

```json
{
  "prec_no": "13",
  "pref_name": "東京都",
  "block_no": "47646",
  "station_name": "...",
  "obs_method": "A",
  "obs_type": "a1",
  "station_name_kana": "...",
  "station_id": "...",
  "latitude": "...",
  "longitude": "...",
  "start_date_raw": "...",
  "start_date": "1994-04-01",
  "available_years_hourly": [1994, 1995, 1998],
  "availability_updated_at": "2026-03-10T00:00:00+00:00"
}
```

補足:

- `station_name_kana`, `latitude`, `longitude` は JMA HTML 一覧ページ由来で保持する
- `station_id`, `start_date` は当面 PDF 由来の既存属性として保持する
- `available_years_hourly` は `jma_availability.py` による URL ベース判定結果を保存する
- JMA の可用年判定に PDF は使わない

### 4.2 WaterInfo

既存観測所オブジェクトに以下を追加する。

```json
{
  "station_id": "1361160200060",
  "station_name": "...",
  "station_name_kana": "...",
  "pref_code": "30",
  "pref_name": "和歌山県",
  "suikei_name": "...",
  "kasen_name": "...",
  "location": "...",
  "latitude": "...",
  "longitude": "...",
  "items": ["雨量"],
  "available_years_hourly": [2001, 2002, 2005],
  "available_years_daily": [1999, 2000, 2001, 2002, 2005],
  "availability_updated_at": "2026-03-10T00:00:00+00:00"
}
```

補足:

- WaterInfo は `available_years_hourly` と `available_years_daily` を両方保持する
- 旧 `river_meta` の `KIND=2/3` availability 取得方針をメンテナンスツール側へ寄せる

### 4.3 スキーマ方針

- 年リストは昇順・重複なし
- `start/end year` は JSON に持たせない
- `availability_updated_at` は各観測所に保持し、「その観測所の可用年を最後に更新した時刻」とする
- 上位の `generated_at` は JSON 全体の生成時刻として維持
- 初版では上位 `availability_generated_at` は必須にしない

## 5. 可用年取得ロジック

### 5.1 JMA

既存 `fetch_available_years_hourly()` を再利用する。JMA の可用年はこの URL ベース判定を正式採用する。

- 入力: `prec_no`, `block_no`
- 出力: `set[int]`
- `indeterminate` の場合はポリシーを決める

補足:

- JMA HTML 一覧ページから `station_name_kana`, `latitude`, `longitude` を取得する
- PDF は `station_id`, `start_date` など HTML で代替できていない属性補完にのみ使う
- `available_years_hourly` の根拠データとして PDF は使わない

推奨ポリシー:

- `rebuild` 時
  - 空で確定させず、既存値があれば維持
  - 既存値がなければ空配列で保存しつつ warning を残す
- `update` 時
  - `indeterminate` なら既存値維持

### 5.2 WaterInfo

既存 `scrape_station()` 全体をそのまま使うのではなく、availability 抽出だけを使える関数へ切り出す。WaterInfo は `hourly` と `daily` の両方を正式対象とする。

想定分離:

- `fetch_waterinfo_available_years(station_id: str, *, kind: int) -> list[int]`
- `fetch_waterinfo_available_years_hourly(station_id: str) -> list[int]`
- `fetch_waterinfo_available_years_daily(station_id: str) -> list[int]`

内部では以下を行う。

- `SrchRainData.exe?ID=...&KIND=...&PAGE=...` を取得
- `parse_availability_page()` で decade table を解析
- ページをたどって年集合を統合

## 6. 更新フロー

### 6.1 WaterInfo

`build_waterinfo_station_index.py` を次の二段階に整理する。

1. ベース観測所メタデータを取得
2. 可用年を取得して各観測所へ付与

実装方法は2案ある。

#### 案A: 同一スクリプト内で一括実行

- 既存 `fetch_site_info()` / `parse_metadata()` の後に availability 更新を続ける
- 利点: ファイル数が増えない
- 欠点: スクリプト責務がやや重い

#### 案B: 可用年更新を別スクリプト化

- `build_waterinfo_station_index.py`: ベース生成
- `update_waterinfo_station_availability.py`: 可用年補完

採用方針:

- WaterInfo は案Bを採用する
- 理由は、ベース生成と availability 更新を独立再実行でき、定期更新時の切り分けが容易になるため

### 6.2 JMA

`update_jma_station_index.py` はすでに PDF 補完責務を持つため、availability 更新も同居させると責務が広がる。

推奨:

- `update_jma_station_index.py`: ベース + PDF 補完
- `update_jma_station_availability.py`: URL ベース可用年補完

### 6.3 統合ランナー

`refresh_station_indexes.py` は最終的に次の順序で呼ぶ。

1. JMA PDF ダウンロード
2. JMA ベース / PDF 更新
3. JMA URL ベース可用年更新
4. WaterInfo ベース更新
5. WaterInfo 可用年更新

`--target` は従来どおり `jma`, `waterinfo`, `all` を持たせる。

## 7. モード設計

### 7.1 rebuild

- 全観測所を再計算
- 既存可用年を信頼しない
- 大規模更新向け

### 7.2 update / resume

- 既存 JSON を読み込む
- 成功済み観測所はスキップまたは再計算ポリシーに従う
- `indeterminate` は既存値維持を基本とする

### 7.3 部分更新オプション

- `--pref`
- `--station-id`
- `--max-count`
- `--sleep`
- `--timeout`

## 8. 保存方針

### 8.1 途中保存

- 100 件ごとなど、固定件数で保存
- 中断時は finally で保存

### 8.2 原子的保存

- `.tmp` に書いてから replace

### 8.3 サマリ項目

上位 JSON に以下があると運用しやすい。

- `availability_generated_at`
- `availability_station_count`
- `availability_success_count`
- `availability_failure_count`

ただし上位メタが肥大化するならログのみでもよい。

初版の採用方針:

- 更新時刻の追跡は各観測所の `availability_updated_at` を正とする
- 上位サマリ項目は必須にしない

## 9. 利用側の移行方針

### 9.1 JMA

`run_rainfall_analyze()` の年絞り込みは、将来的には次の優先順位に寄せる。

1. JSON の `available_years_hourly`
2. 移行期間のみ、必要ならオンライン判定

最終的には 2 を廃止し、更新済み JSON を唯一の参照元にする。

### 9.2 WaterInfo

WaterInfo は現状「観測所別の年可用性判定を行わない」扱いなので、JSON 化後に改善できる。

想定:

- 対象年を JSON の `available_years_hourly` / `available_years_daily` で絞る
- 不要な年の取得を減らす

方針:

- 通常利用時の可用年判定は更新済み JSON を正本とする
- オンライン availability 判定は移行期間の暫定フォールバックに留める

## 10. テスト方針

- availability ページ解析のユニットテスト
- 既存 JSON へのマージテスト
- `indeterminate` / timeout のフォールバックテスト
- 途中保存と resume のテスト
- `refresh_station_indexes.py` のコマンド組立テスト
