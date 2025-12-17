# Python/uvで実行

## 事前準備

- Python: `>=3.12`
- パッケージ管理: `uv`

## セットアップ

```powershell
uv sync
```

## 起動（ランチャー）

```powershell
uv run python main.py
```

## モジュール単体で起動

```powershell
# 国交省 水文データ取得
uv run python -m water_info

# 気象庁 雨量データ取得
uv run python -m jma_rainfall_pipeline
```

## テスト

```powershell
uv run pytest
```

