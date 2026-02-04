# water_info リファクタリング記録（責務分離）

## 概要
今回の作業は、`src/water_info` における **責務分離** を目的に行った。
UI / Service / Domain / Infra を分け、取得・整形・出力の共通化とテスト基盤の整備を進めた。

## 何をしたか（要点）
- **UI分離**: Tkinter UI を `src/water_info/ui/app.py` に移動し、`main_datetime.py` から切り離し
- **Service導入**: `service/usecase.py` を追加し UI からの入口を集約
- **Domain導入**: `domain/models.py` で `Period/Options/WaterInfoRequest` を定義し、バリデーションを一元化
- **Infra共通化**: HTTP・スクレイピング・URL生成・Excel出力・DataFrame整形を `infra/` に分離
- **取得処理共通化**: 観測所名取得/値抽出/HTML取得を共通関数化
- **UI入力検証の整理**: domain バリデーションに統一し、即時エラーはフォーム内に表示
- **テスト追加**: ユーティリティ/URL/整形/出力/バリデーション/スクレイピングのテストを追加

## 新規・更新された主要ファイル
### UI
- `src/water_info/ui/app.py`（新規）

### Service
- `src/water_info/service/usecase.py`（追加・整理）

### Domain
- `src/water_info/domain/models.py`（新規）

### Infra（共通化）
- `src/water_info/infra/http_client.py`
- `src/water_info/infra/http_html.py`
- `src/water_info/infra/url_builder.py`
- `src/water_info/infra/scrape_station.py`
- `src/water_info/infra/scrape_values.py`
- `src/water_info/infra/fetching.py`
- `src/water_info/infra/dataframe_utils.py`
- `src/water_info/infra/excel_writer.py`
- `src/water_info/infra/excel_summary.py`

### テスト
- `tests/water_info/test_date_utils.py`
- `tests/water_info/test_http_delay.py`
- `tests/water_info/test_url_build.py`
- `tests/water_info/test_dataframe_logic.py`
- `tests/water_info/test_domain_validation.py`
- `tests/water_info/test_usecase_fetch_for_code.py`
- `tests/water_info/test_scrape_smoke.py`
- `pytest.ini`

## アーキテクチャの要点（責務分離）
- **UI**: 入力・表示・イベントのみ。処理は usecase に委譲
- **Service**: 取得フローの統制。1件処理/複数件処理を分離
- **Domain**: 期間/モード/オプションの検証と入力仕様の確定
- **Infra**: 外部依存（HTTP, HTML, Excel, pandas）を集約

## 現在の動作仕様（変更なし）
- 取得・出力フォーマット（Excel構成、シート名、チャート構成）は維持
- `python -m src.water_info` の起動経路は維持

## テスト実行
- `uv run pytest -q`
- 現在のテスト件数: **23件**

## 今後の候補
- `service` をユースケース単位にさらに分割（取得/出力の責務整理）
- スクレイピングの堅牢化（HTML変化検知）
- UIの入力エラー表示の見た目調整
