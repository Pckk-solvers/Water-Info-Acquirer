# 時刻契約統一 実装レディネス判定（2026-04-06）

Status: archived
Updated: 2026-04-06
Related:
- `./datetime-internal-unification-requirements.md`
- `./datetime-internal-unification-impact-analysis.md`
- `./datetime-internal-unification-task-breakdown.md`

## 1. 目的
- `target` にある `T1〜T10` について、現実装の到達点を簡潔に可視化する。
- 次アクションを「ユーザー主導で決めるべき論点」と「実装を先行できる作業」に分離し、着手判断をしやすくする。

## 2. 全体サマリ（結論）
- 全体進捗の目安: **約45%**
- 先行完了済み:
  - Excel 時刻契約（`period_end_at` 優先、年別標準、全期間オプション）は反映済み
  - JMA/WaterInfo の出力経路で年別+全期間の実データ確認済み
- 未完了の本丸:
  - `display_dt` 完全撤去
  - `request_failed` の実行ログ運用固定
  - Graph/Analysis の `period_end_at` 完全統一
  - 回帰テスト再定義と旧ロジック撤去

## 3. T1〜T10 判定（簡易）

| Task | 状態 | 判定理由（要点） | 推奨担当 |
|---|---|---|---|
| T1 契約固定 | ほぼ完了 | `river_meta.rainfall` のモデルに `period_*` が入り、`observed_at` 同期も実装済み | こちらで仕上げ可 |
| T2 正規化共通化 | ほぼ完了 | `normalize_period` が導入済み。24時相当の正規化経路あり | こちらで仕上げ可 |
| T3 取得境界統一 | 部分対応 | sourceごとにフィルタはあるが、全経路で統一契約として固定し切れていない | **ユーザー判断が先** |
| T4 Parquet契約移行 | ほぼ完了 | `period_*` を含む保存/読込互換が実装済み | こちらで仕上げ可 |
| T5 CSV/Excel置換 | ほぼ完了 | `period_end_at` 優先表示、年別/全期間シート制御が反映済み | こちらで仕上げ可 |
| T6 Graph/Analysis置換 | 部分対応 | 一部は `period_end_at` 寄せ済みだが、全グラフ経路の統一が未完 | **ユーザー判断が先** |
| T7 `display_dt`除去 | 未完 | `water_info` 系に `display_at` 系中間列が残る | **ユーザー判断が先** |
| T8 失敗分類統一 | 未完 | `request_failed` を「ログのみ」に固定する運用が未完 | **ユーザー判断が先** |
| T9 回帰テスト再定義 | 未完 | 契約更新後の横断テスト網が不足 | こちらで先行可 |
| T10 旧ロジック撤去 | 未完 | `drop_last_each` 等の完全撤去と完了宣言が未実施 | **ユーザー判断が先** |

## 4. ユーザー判断なしで固定した論点（今回決定）

### D-01 時刻境界の最終ルール固定（T3/T6）
- **決定**:
  - 出力最終判定は全 source で `period_end_at` 基準に統一する。
  - 瞬間値（S/R）は `period_end_at` が `NULL` の場合のみ `observed_at` を使用する。
- この方針で Graph/Analysis 側の時刻軸と窓判定を統一実装する。

### D-02 `display_dt` 廃止の完遂方針（T7/T10）
- **決定**:
  - `display_at` / `display_dt` は出力直前派生のみに限定し、中間保持を禁止する。
  - 互換期間は設けず、主経路は即時置換する（旧互換は読込時の最小限補正のみ）。

### D-03 `request_failed` の運用固定（T8）
- **決定**:
  - `request_failed` は実行ログのみで管理し、データレコード（Parquet/CSV/Excel）へは保存しない。
  - ログ項目は task-breakdown の最低項目を固定採用する。
  - UI 表示は `missing` と `request_failed` を明示分離する。

## 5. こちらで先行実装できる作業
- P-01 `T1/T2/T4/T5` の残差分を潰して、実装と文書を同期
- P-02 `T9` の回帰テスト拡充（月初00:00、24時相当、年跨ぎ）
- P-03 `T6` のうち、方針確定不要な機械置換（`period_end_at` 参照への揃え込み）

## 6. 推奨進め方（最短）
1. 今回確定した D-01〜D-03 を前提に、こちらで P-01〜P-03 を一括実装
2. 次に `T7/T8/T10` をまとめてクローズ
3. 完了判定後に `target -> archive` へ移行

---

この文書は「実装着手可否の判断用」なので、詳細仕様は `target` 側を正本として扱う。

