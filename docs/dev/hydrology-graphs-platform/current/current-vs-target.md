# Hydrology Graphs Platform Current vs Target

Status: current
Updated: 2026-04-01

## 1. 結論
- 2026-04-01 時点で、旧 `target` 文書は `archive` へ移動済み。
- 主要仕様（style v2.0、9種スタイル単位、UI連動、出力パス、ランチャー統合）は `current` と整合。

## 2. 整合確認サマリ

| 区分 | current | target | 判定 |
|---|---|---|---|
| スタイル契約 | `schema_version=2.0`、`graph_styles` 9キー、`common/variants` 禁止 | 旧target文書（archive）で同等記載 | 整合 |
| スタイル適用単位 | 9種（イベント3種×3日/5日 + 年最大3種） | 旧target文書（archive）で同等記載 | 整合 |
| プレビュー対象 | グラフ種別は「スタイル編集対象」に連動 | 旧target文書（archive）で同等記載 | 整合 |
| 基準日入力 | 改行区切り `YYYY-MM-DD` | 旧target文書（archive）で同等記載 | 整合 |
| 年最大条件 | 10年以上 | 旧target文書（archive）で同等記載 | 整合 |
| 出力パス | `<output_dir>/<station_key>/<graph_type>/<base|annual>/graph.png` | 旧target文書（archive）で同等記載 | 整合 |
| ランチャー統合 | 実装済み | 旧target文書（archive）で同等記載 | 整合 |

## 3. 残タスク（文書運用）
1. `archive/style-contract-v1.md` は履歴文書として保持（legacy）。
2. 今後は仕様変更ごとに `current` を先に更新し、新規計画が必要なときのみ `target` を作る。

## 4. 参照
- [current/spec.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/current/spec.md)
- [archive/requirements.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/archive/requirements.md)
- [archive/design.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/archive/design.md)
- [archive/layout.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/archive/layout.md)
- [archive/style-contract.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/archive/style-contract.md)
