## Water-Info-Acquirer(water Information Acquisition)；水文データ取得・整理ツール
水文水質データベースからデータから水位・流量・雨量データを指定期間ごとに取得する。
取得したデータを日単位または1時間単位でExcelへ整理し、ハイドログラフを挿入する。

配布形式は実行ファイルのため Windows OS であれば即実行可能<br>
## クイックスタート
実行ファイルをダウンロードしてすぐに使用したい人向けとなります。

### ツールをダウンロード
ツールは[Releases](https://github.com/Pckk-solvers/Water-Info-Acquirer/releases)へバージョンごとに管理しています。<br>
最新版の`.zip`をダウンロードしてください。

### ツールを展開
ダウンロードしたツールを任意の場所へ展開します。<br>

### ダブルクリックで実行
`.exe`をダブルクリックで実行します。実行環境によっては画面の立ち上がりまで30秒ほどかかる場合がありますので、ご了承ください。<br>
また、プライバシーや権限等のセキュリティーに制限される場合もございますが、こちらのツールがそれらを侵害することはありませんので、ご安心ください。<br>


## Pythonモジュールとして実行する場合
こちらはすでにPython環境が構築している方や開発者向けの実行手順となります。<br>
リポジトリをローカルへクローンした後の手順を説明します。

### ※Python3.13以降のバージョンを推奨いたします。
クローンしたディレクトリへ移動する。
```bash
cd クローン先のディレクトリ
```

#### 推奨（uv）
依存関係をインストールする。
```bash
uv sync
```
起動する。
```bash
uv run python main.py
```

#### 代替（pip）
```bash
pip install -r requirements.txt
python main.py
```
コンソール画面には詳しく、デバックしているので適宜ご確認ください。<br>
もし、改善点等がありましたらお問い合わせいただけますと幸いです。<br>

## ドキュメント（MkDocs）
ローカルでドキュメントサイトを起動できます。
```bash
uv run mkdocs serve
```

公開サイト（GitHub Pages）: https://pckk-solvers.github.io/Water-Info-Acquirer/  
公開手順の詳細: `docs/dev/github-pages.md`

GitHub Pages（プロジェクトページ）へ公開する場合は、リポジトリの **Settings → Pages → Source** を **GitHub Actions** に設定してください。  
以降は `main` への push で自動デプロイされます（`.github/workflows/docs.yml`）。


### 実行後のエラーケース
実行ができてもエラーが確認されることがございます。<br>
こちらについては基本的にはデータ元が存在しないケースが該当します。<br>
そのため、まずは短期間でのデータ取得を試み、成功したら期間を延ばすという流れが推奨されます。


### exe版はReleasesをご参照ください
Actionを用いて自動でビルドとリリースを行います。
