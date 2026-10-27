# ダム取得と画面再構成のブランチ分離（完了）

## 目的・承認

ユーザーの明示的な指示に基づき、ダムの取得・品質保持・年別シート出力と旧画面からのダム利用をmainへコミット・pushし、画面再構成だけを別ブランチへコミット・pushする。

## 対象

既存の未コミット変更、`src/water_info/`、関連テスト、`docs/dev/` と利用説明。生成済みデータはローカルに保持し、Gitには含めない。

## 実施内容

1. リモートと差分を確認し、変更ファイルのバイト単位バックアップを作る。
2. mainは従来画面にダム対象の選択と独立パネルだけを加えた状態にし、機能・文書・テストを同期して検証する。
3. mainをpushし、そこから `feature/water-info-gui-redesign` を作成する。
4. 保存した再構成済み画面を復元し、mainとの差分が画面再構成・共通実行・出力先接続・説明・関連テストに限られることを確認してpushする。

## 完了条件・確認方法

両ブランチのwater_infoテスト、ruff/対象pyright、文書リンク、Git差分を確認し、リモートのHEADとローカルのコミットが一致する。作業終了時は画面再構成ブランチに置く。

## 関連要件・設計

- `docs/dev/requirements/water_info-dam.md`、`docs/dev/architecture/water_info_dam.md`
- `docs/dev/requirements/water_info-gui.md`、`docs/dev/architecture/water_info_gui.md`（画面再構成ブランチ）

## 自己レビュー

- 未コミットの作業と生成データを保持する: OK、ファイル内容とハッシュを保存する。
- main単独の動作: OK、GUI再構成用モジュールへの依存を置かず検証する。
- ブランチ関係: OK、UIブランチはダム機能コミットから分岐させる。
- 公開範囲: OK、通常のpushのみ。force pushは使わない。
- 承認: 本ターンのユーザー指示によりコミットと両ブランチのpushが承認済み。

## 結果・検証

- main: `374fe4e`。ダム専用の取得・品質保持・1冊の年別シート出力・従来画面の対象切替とダムパネルを収録。
- mainのwater_infoテスト65件passed。コミット予定ファイルだけをcheckout-indexで隔離した状態でも65件passed。ダムモジュールのruff/pyright、文書リンクを確認した。
- 画面再構成: `feature/water-info-gui-redesign`。上記mainから分岐し、共有出力先・実行、条件フォーム、配置、結果表示、説明、画像、関連テストを収録。
- 退避した38ファイルはバイト単位のハッシュで一致を確認して復元。ダムのdomain/parser/writer/service、保守スクリプトと取得テストはmainとの差分なし。
- UI側のwater_infoテスト73件passed。ruff/対象pyright passed、文書リンク確認済み。
- 公開先はorigin/mainとorigin/feature/water-info-gui-redesign。通常のpushを使用する。作業終了時はUIブランチをチェックアウトする。
- 生成済みデータ・再整理ファイル・退避用スナップショットはoutputs配下に保持し、コミット対象外とした。
