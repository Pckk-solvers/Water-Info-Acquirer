# style JSON Schema を src 正本へ移行

## 目的
- style 契約の正本を `src` 配下の JSON Schema ファイルに移す。
- `style_store.py` の契約検証で JSON Schema を参照し、実装と仕様の二重管理を減らす。

## 対象ファイル
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- `src/hydrology_graphs/io/style_store.py`
- `tests/hydrology_graphs/test_style_store.py`
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`

## 実施内容
- `style_schema_2_0.json` を新規追加する。
- `style_store.py` で JSON Schema を読み込み、契約検証に使う。
- 既存 warning 互換を維持しつつ、構造検証をスキーマ駆動へ寄せる。
- 関連文書に正本パスを追記する。

## 完了条件
- style 読込時に JSON Schema を参照して契約検証される。
- 既存の style_store テストが通る。
- 参照文書から正本スキーマファイルへ辿れる。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_style_store.py`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 実施結果（途中）
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json` を追加し、`style_store.py` から参照するようにした。
- 値制約（色形式、enum、正値/非負、tick関連）を JSON Schema 側へ移し、`style_store.py` は正規化・互換・エラーコード変換を担当する構成に変更した。
- `display.time_display_mode` は互換のため事前正規化してから schema 検証するようにした。
- `uv run pytest -q tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_services.py` で通過した。

## 完了結果
- style JSON の正本を `src/hydrology_graphs/io/schemas/style_schema_2_0.json` に一本化した。
- style 読込/保存経路で JSON Schema 検証が実行されることを確認した。
- 要件/設計/参照文書に正本スキーマと検証方針を反映した。
