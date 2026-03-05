# 整理・出力タブ 性能改善 タスク

## Phase 1（Excel高速化）
1. 観測所単位バッチ化
- `run_rainfall_generate()` を観測所グループ単位へ再構成
- Excel出力を観測所ごと1回に統合

2. 列幅調整コスト削減
- `excel_exporter.py` の列幅調整を1回化

3. 回帰テスト
- 既存 `tests/river_meta` を実行し互換確認

## Phase 2（差分最適化）
1. manifest基盤
- `output_dir/metadata/excel_manifest.json` の読み書き実装
- 破損時フォールバック実装

2. Excel差分更新
- 観測所×年 digest 判定
- 変更なし観測所スキップ
- 変更あり観測所のみ再生成

3. グラフ差分生成
- 既存PNG + digest一致のスキップ
- 未生成/変更分のみ生成

4. ログ整備
- 生成/スキップ件数、観測所処理時間、全体時間
- モード情報（diff_mode / force_full_regenerate）

5. GUI最小構成
- `差分更新を使う`（既定ON）を追加
- `全再生成する`（既定OFF）を追加
- `全再生成する` が最優先で動作するよう配線

## Phase 3（品質保証）
1. 単体テスト追加
- manifest I/O
- digest判定
- Excel差分/グラフ差分

2. 結合テスト追加
- 再実行でスキップが効くこと
- 変更年だけ再生成されること
- `force_full_regenerate=true` でフル再生成されること
- 既定値（diff=ON, force=OFF）が有効であること

3. 回帰試験
- `uv run pytest tests/river_meta -q`

## リスク
1. manifestと実ファイルの不整合
- 常に実ファイル存在確認を併用し、欠損時は再生成する。

2. Excel差分更新の重複行
- 初期実装は観測所単位「再生成」に限定して重複リスクを排除する。
