# 水文データ取得ツール - 移行完了

## 概要

このプロジェクトは新しいパッケージ構造に移行されました。

## 新しい実行方法

### コマンドライン実行
```bash
python -m src
```

### ヘルプの表示
```bash
python -m src --help
```

### シングルシートモード
```bash
python -m src --single-sheet
```

## ビルド方法

### PyInstallerでのビルド
```bash
pyinstaller Water-Info-Acquirer-1.0.spec
```

または

```bash
pyinstaller --onefile --windowed --name "WaterInfoAcquirer" src/__main__.py
```

## テスト実行

### 基本テスト
```bash
python -m pytest tests/ -v
```

### カバレッジ付きテスト
```bash
python -m pytest tests/ --cov=src/wia --cov-report=term-missing --cov-report=html
```

### テストランナー使用
```bash
python test_runner.py
```

## パッケージ構造

```
src/
├── __init__.py          # パッケージエントリーポイント
├── __main__.py          # CLIエントリーポイント
└── wia/                 # メインパッケージ
    ├── __init__.py      # wiaパッケージ初期化
    ├── api.py           # 統合実行API
    ├── constants.py     # 定数定義
    ├── data_source.py   # データ取得処理
    ├── errors.py        # カスタム例外
    ├── excel_writer.py  # Excel出力処理
    ├── exception_handler.py # 例外ハンドリング
    ├── gui.py           # GUI実装
    ├── logging_config.py # ログ設定
    └── types.py         # 型定義
```

## 削除されたファイル

- `main.py` - 旧実装のエントリーポイント（新しい `src/__main__.py` に置き換え）

## 注意事項

- 全てのimport文は相対インポートに変更されました
- テスト環境にはpytest、pytest-mock、pytest-covが含まれています
- ログ設定とカバレッジ設定が統合されています