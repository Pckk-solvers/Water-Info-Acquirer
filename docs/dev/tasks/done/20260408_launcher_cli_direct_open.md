# main.py からアプリ直接起動を可能にする

## 目的
- ランチャー画面を経由せず、CLI引数で指定したアプリを直接起動できるようにする。

## 対象ファイル
- `main.py`
- `src/water_info_acquirer/launcher.py`
- `docs/dev/architecture/entrypoints.md`
- `tests/test_main_entry.py`

## 実施内容
- `main.py` にアプリ指定引数（4アプリのキー）を追加する。
- 指定時は `launcher.main(..., launch_target=...)` で直接アプリを開く。
- 指定なし時は従来通りランチャー画面を開く。
- 起動経路ドキュメントを更新する。
- CLI引数解決のテストを追加する。

## 完了条件
- `python main.py --app <key>` または `python main.py <key>` で指定アプリが直接起動する。
- `python main.py` は従来どおりランチャーを開く。

## 確認方法
- `uv run pytest -q tests/test_main_entry.py`

## 関連要件 / 関連設計
- 設計: `docs/dev/architecture/entrypoints.md`

## 完了結果
- `main.py` に `--app` と位置引数での直接起動指定を追加した。
- `src/water_info_acquirer/launcher.py` に `launch_target` を追加し、指定時はランチャー画面を出さず直接起動するようにした。
- `docs/dev/architecture/entrypoints.md` に新しい起動経路を反映した。
- `tests/test_main_entry.py` を追加し、引数解決の挙動を確認できるようにした。
