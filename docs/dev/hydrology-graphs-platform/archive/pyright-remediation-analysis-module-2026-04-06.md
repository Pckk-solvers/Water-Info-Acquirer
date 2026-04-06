# Pyright エラー解消ガイド（river_meta analysis）

Status: archived
Updated: 2026-04-06
Target:
- `src/river_meta/rainfall/outputs/analysis.py`
- `tests/river_meta/test_rainfall_analysis.py`

## 1. 目的
- `analysis.py` を含む `pyright` 実行時に発生する型エラー群を、機能影響を変えずに解消する。
- pandas 実装における `Unknown/None` 連鎖を止め、継続的に `pyright` を回せる状態へ戻す。

## 2. 現状のエラー分類

### A. `DataFrame.get(...)` 起点の `Unknown | None`
- 症状:
  - `to_datetime(prepared.get(...))`
  - `prepared.get(...).astype(...)`
  - その後の `.where/.loc/.dropna` で連鎖エラー
- 原因:
  - `get` は型が弱く `None` も返すため、pyright が Series と確定できない。

### B. `DataFrame(columns=[...])` の型不一致
- 症状:
  - `list[str]` を `columns` に渡した箇所で `Axes` 期待との不一致警告。
- 原因:
  - pandas の型スタブとの厳密整合不足。

### C. テストの `datetime | NaTType` 不一致
- 症状:
  - `datetime | NaTType` を `datetime | None` に渡す箇所でエラー。
- 原因:
  - `pd.to_datetime` 系の戻りに `NaT` が残るまま dataclass へ渡している。

## 3. 修正パターン（固定）

### P-01 `get` を避け、列存在分岐で Series を確定
- NG:
  - `prepared.get("period_end_at")`
- OK:
  - `if "period_end_at" in prepared.columns: period_end = cast(pd.Series, prepared["period_end_at"]) else: period_end = pd.Series(pd.NaT, index=prepared.index)`

### P-02 `to_datetime`/`to_numeric` は `Series` を明示 `cast`
- `cast(pd.Series, ...)` で型を固定してから変換。
- 変換後は同じ列へ再代入し、以降はその列を参照する。

### P-03 空 DataFrame は `pd.Index([...])` を使って返す
- NG:
  - `pd.DataFrame(columns=TIMESERIES_COLUMNS)`
- OK:
  - `pd.DataFrame(columns=pd.Index(TIMESERIES_COLUMNS))`

### P-04 `quality` の map は `Series` 化して処理
- `quality_series = cast(pd.Series, aligned["quality"])`
- `mapped = quality_series.map({...})` を使い、`fillna` 後に代入。

### P-05 テストの `NaT` は `None` に正規化して渡す
- dataclass に渡す前に:
  - `value = pd.to_datetime(..., errors="coerce")`
  - `value = None if pd.isna(value) else value.to_pydatetime()`

## 4. 実装順（推奨）
1. `analysis.py` の `build_hourly_timeseries_dataframe` を P-01/P-02 で整理
2. `_build_station_timeseries` を P-02/P-04 で整理
3. 空戻り DataFrame を P-03 へ統一
4. `test_rainfall_analysis.py` の `NaT` ケースを P-05 で修正
5. `ruff` → `pyright` → `pytest` の順で検証

## 5. 受け入れ条件
- `uv run pyright src/river_meta/rainfall/outputs/analysis.py tests/river_meta/test_rainfall_analysis.py` が 0 error
- `uv run pytest tests/river_meta/test_rainfall_analysis.py` が全通過
- 既存の出力仕様（列名、件数、ソート順、年別集計）が変わらない

## 6. 実行コマンド
- `uv run ruff check src/river_meta/rainfall/outputs/analysis.py tests/river_meta/test_rainfall_analysis.py`
- `uv run pyright src/river_meta/rainfall/outputs/analysis.py tests/river_meta/test_rainfall_analysis.py`
- `uv run pytest tests/river_meta/test_rainfall_analysis.py -q`

