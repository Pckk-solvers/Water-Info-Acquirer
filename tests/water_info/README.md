# テスト概要

本ディレクトリには `water_info` のリファクタリング安全性を担保するテストを配置しています。
各テストの目的は以下の通りです。

- `test_date_utils.py`
  - `month_floor` / `shift_month` の月跨ぎロジック確認

- `test_http_delay.py`
  - HTTPスロットリングの遅延計算（最小/最大）を確認

- `test_url_build.py`
  - 生成されるURLの `KIND` / `Dsp*Data` / パラメータ整合性を確認

- `test_dataframe_logic.py`
  - `display_dt` の+1hシフト、`sheet_year` 判定、空データ時例外を確認
  - Excel出力の最低限スモーク（シート名）も確認

- `test_domain_validation.py`
  - `Period/WaterInfoRequest` の入力検証（年/月/期間/モード）を確認

- `test_usecase_fetch_for_code.py`
  - `fetch_for_code` の成功/失敗時の戻り値を確認

- `test_scrape_smoke.py`
  - HTMLスクレイピングの最小動作（観測所名抽出/値抽出）を確認

- `test_service_flow.py`
  - `flow_fetch/flow_write` の基本動作（DF生成・出力）を確認

- `test_fetching_drop_last_each.py`
  - 月ごとの末尾1件削除ロジック（時間データのズレ防止）を確認

実行方法: `uv run pytest -q`
