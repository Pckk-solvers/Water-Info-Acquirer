# river_meta rainfall architecture

`src/river_meta/rainfall/` は、JMA と Water Info の雨量データを集め、正規化し、分析・出力する。

## 入口

- `src/river_meta/rainfall/__main__.py`
  - `python -m river_meta.rainfall` 系の起動入口。
- `src/river_meta/rainfall/entry.py`
  - 共通実行入口。
- `src/river_meta/rainfall/cli.py`
  - CLI の解釈。
- `src/river_meta/rainfall/gui/launcher_entry.py`
  - アプリ選択ランチャーから開く入口。

## 責務

- 観測所の解決
- JMA / Water Info の雨量収集
- データの正規化と統合
- 期間別の集計・分析
- Excel / chart / parquet 出力
- GUI からの生成・収集・期間出力操作

## 主な層

- `sources/`
  - JMA / Water Info の取得アダプタと観測所解決を置く。
- `services/`
  - 収集、生成、分析、期間出力のユースケースを置く。
- `domain/`
  - 正規化ルール、モデル、ユースケース用モデルを置く。
- `storage/`
  - Parquet ストアと manifest を置く。
- `outputs/`
  - Excel、chart、分析出力を置く。
- `gui/`
  - 収集、生成、期間出力、補助表示の画面を置く。
- `support/`
  - 期間操作や共通補助を置く。
- `commands/`
  - 補助コマンドを置く。

## データの流れ

1. GUI / CLI が対象期間と観測所を受け取る。
2. `services/collect.py` が JMA / Water Info の入力を集める。
3. `domain/normalizer.py` がデータの時刻意味を揃える。
4. `services/generate.py` と `services/analyze.py` が集計を行う。
5. `storage/` と `outputs/` が parquet / Excel / chart に落とす。

## 補足

- `period_end_at` を中心に扱う。
- 生成物は source 別に分けつつ、共通の期間ロジックを使う。
- GUI は生成・収集・期間出力をタブで分けている。
