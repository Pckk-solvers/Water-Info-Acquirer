# Water Info CLI Validation

`water_info` の実データ確認では、CLI から同一条件で `JSONL` / `Excel` / `Parquet` を同時に出力し、サイトの HTML と見比べる。

## 目的
- 取得結果をファイルとして残す。
- Excel 表示、Parquet 保存、JSON 系出力の時刻解釈を揃えて確認する。
- 24時相当が翌日 `00:00:00` に正規化されているかを目視で確認する。

## 実行例

```powershell
uv run python -m src.water_info fetch `
  --code 303031283301020 `
  --mode S `
  --start 2024-01 `
  --end 2024-01 `
  --interval hourly `
  --ndjson `
  --parquet `
  --output-dir outputs/water_info/validation/303031283301020_202401
```

### 補足
- `--excel` は既定で有効なので、通常は明示不要。
- 標準出力にはコードごとの JSON サマリが出る。
- `--ndjson` を付けると、複数コード時の統合 JSONL が出る。
- 出力先は `--output-dir` で固定すると比較しやすい。

## 確認物
- `*.xlsx`: Excel で開いて時刻列と値列を確認する。
- `*.parquet`: `period_end_at` / `observed_at` の中身を確認する。
- `*.ndjson`: JSON 形式で行ごとの時刻と値を確認する。
- 標準出力の JSON: 生成ファイル名とコードの対応を確認する。

## 比較観点
- HTML の 1 行目と出力の 1 行目が一致しているか。
- `24:00` の値が翌日 `00:00:00` に落ちているか。
- 時刻列と値列が同じ行の値になっているか。
- Excel と Parquet で同じ時刻が使われているか。

## 実施メモ
- 先に HTML 側の row ベース抽出を確認する。
- その後、CLI 出力の `Excel` / `Parquet` / `JSONL` を同じ期間で突き合わせる。
- 差分があれば、まず HTML の行構造とパース条件を見直す。
