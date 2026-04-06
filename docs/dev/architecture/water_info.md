# Water Info architecture

`src/water_info/` は、水文水質データの取得・正規化・出力を担当する。

## 入口

- `src/water_info/__main__.py`
  - `python -m water_info` の起動入口。
- `src/water_info/entry.py`
  - 取得・出力の共通実行入口。
- `src/water_info/cli.py`
  - CLI 引数の解釈。
- `src/water_info/launcher_entry.py`
  - `water_info_acquirer` から開くときの GUI 入口。

## 責務

- 取得条件の解釈
- 国交省サイトからの取得
- 観測所・値・時刻の正規化
- Parquet / NDJSON / Excel 出力
- GUI での条件設定と実行

## 主な層

- `domain/`
  - データモデルと、業務上の意味を持つ型を置く。
- `service/`
  - 取得、書き込み、実行制御のユースケースを置く。
- `infra/`
  - HTTP、HTML パース、URL 組み立て、Excel 書き込みなどの実装詳細を置く。
- `ui/`
  - Tkinter の画面、ダイアログ、進捗表示、実行操作を置く。

## データの流れ

1. UI または CLI が入力条件を受け取る。
2. `service/usecase.py` が取得単位と出力先を決める。
3. `service/flow_fetch.py` がリクエスト窓を計算する。
4. `infra/fetching.py` と関連モジュールが HTML / 表を取得する。
5. `entry.py` が標準スキーマへまとめる。
6. `service/flow_write.py` と `infra/excel_writer.py` が各形式へ書き出す。

## 補足

- `period_end_at` が時刻の正本。
- `observed_at` は欠損時の補助参照。
- 表示用の別時刻列を中間に恒久保存しない。
