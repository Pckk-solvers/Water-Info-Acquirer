# Excel出力: 時間範囲と年シート分割 契約定義

Status: archived
Updated: 2026-04-06
Related:
- `docs/dev/time-semantics-and-fetch-window-design.md`
- `docs/dev/hydrology-graphs-platform/target/datetime-internal-unification-requirements.md`

## 1. 目的
- `water_info` と `jma_rainfall_pipeline` の Excel 出力で、時間範囲の切り方とシート分割ルールを統一する。
- Excel 出力専用の局所補正を避け、`publish_window` 後の共通レコードをそのまま使う。

## 2. スコープ
- 対象:
  - `water_info` の Excel 出力（hourly / daily）
  - `jma_rainfall_pipeline` の Excel 出力（10min / hourly / daily）
  - 年シート分割と全期間シート出力条件
- 非対象:
  - グラフ描画の見た目調整
  - 値列の統計ロジック変更（平均、位況、ランキング）

## 3. 決定事項（固定）
1. Excel 表示時刻は `period_end_at` を固定で使用する。  
   - 瞬間値 (`water_info:S/R`) は `period_end_at` が `NULL` になり得るため、内部で `observed_at` を `period_end_at` 相当として解決してから書き込む。
2. 先頭の `00:00` レコードは Excel に含める。
3. シート構成は「年別シートを基本」とし、「全期間シート」は指定時のみ出力する。

## 4. 用語
- `request_window`: ユーザーが指定した論理期間
- `fetch_window`: 境界補完のために拡張した取得期間
- `publish_window`: 最終出力対象期間
- `excel_display_at`: Excel 用に確定した表示時刻（本契約では `period_end_at`）

## 5. 時刻列契約

### 5.1 共通
- Excel 出力前に、各レコードで `excel_display_at` を確定する。
- 確定ルール:
  - `period_end_at` がある場合: `excel_display_at = period_end_at`
  - `period_end_at` がない場合: `excel_display_at = observed_at`
- Excel の日時列は `excel_display_at` だけを使って生成する。

### 5.2 24時相当
- 内部では `24:00` を保持しない。
- `24:00` 相当は翌日 `00:00:00` として保持し、Excel も同じ時刻値を使う。
- 表示上の `24:00` 変換を行う場合でも、出力直前の表示フォーマット処理に限定する。

## 6. 範囲フィルタ契約
- Excel へ渡すレコード集合は、`publish_window` 適用後のものだけを使う。
- 判定式（確定）:
  - 瞬間値: `request_start <= observed_at <= request_end_exclusive`
  - 区間値: `request_start <= period_end_at <= request_end_exclusive`
- 先頭 `00:00` レコードは上記条件で `== request_start` を許容し、出力対象に含める。

## 7. 年シート分割契約

### 7.1 分割キー
- 年シート分割は `excel_display_at.year` で行う。
- 例:
  - `2026-03-04 00:00:00` は 2026 年シートに入る。

### 7.2 標準構成
- デフォルトは年別シートのみ出力する。
- 年別シートは年昇順で作成する（`2024年`, `2025年`, ...）。

### 7.3 全期間シート
- 全期間シートはオプション指定時のみ出力する。
- 指定がない場合は作成しない。
- 全期間シートを作る場合も、データ集合は年別シートと同じ（`publish_window` 後）を使う。
- JMA GUI では `Excelに全期間シートを追加` チェックボックスで制御する（既定: OFF）。

## 8. 実装ルール
1. `water_info` / `jma` それぞれで時刻補正を重複実装しない。  
   - 共通の Excel 時刻解決関数を使う。
2. Excel 出力前に `excel_display_at` を1回だけ確定する。
3. 年分割ロジックは共通化する（分割キー・ソート順・空シート扱い）。
4. 出力層で `fetch_window` を再解釈しない（再計算しない）。

## 9. 受け入れ条件
1. `water_info:S/R` で `period_end_at` が `NULL` でも、Excel の時刻列は欠けずに出力される。
2. `water_info:U` / `jma` で先頭 `00:00` 区間が Excel に含まれる。
3. 年シート分割結果が `excel_display_at.year` と一致する。
4. 全期間シート OFF のとき、年シートのみが生成される。
5. 全期間シート ON のとき、年シート + 全期間シートが同一レコード集合で生成される。

## 10. 検証項目
- `water_info:S hourly`:
  - 先頭 `observed_at=00:00` が Excel 先頭に出る
- `water_info:U hourly`:
  - 先頭 `period_end_at=00:00` 区間が Excel に出る
- `jma hourly/10min`:
  - 先頭 `period_end_at=00:00` 区間が Excel に出る
  - 末尾 `24時相当` は翌日 `00:00` で保持される
- 年跨ぎケース:
  - 年シート分割が `excel_display_at.year` で正しく振り分けられる

## 11. 保留事項
1. Excel 表示上で `00:00` を `24:00` へ変換するか（現在は未適用を前提）
2. 全期間シート名の固定文字列（`全期間` 固定か、設定可能か）
