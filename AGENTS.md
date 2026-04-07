英語で思考してユーザへの回答などは日本語で必ずすること。
実行をする場合はuv run を使ってPythonは実行する。

# AGENTS.md

## Purpose
このリポジトリは、アプリケーション本体の実装と、その開発・運用に必要な文書を一貫して管理する。
エージェントは、コードを変更する前に `docs/dev/` の内容を確認し、仕様・設計・作業計画と整合した変更を行うこと。

---

## Source of truth
このリポジトリにおける開発用ドキュメントの正本は `docs/dev/` 配下に置く。

- 要件: `docs/dev/requirements/`
- 設計: `docs/dev/architecture/`
- 設計判断の記録: `docs/dev/adr/`
- ドメイン知識・制約: `docs/dev/domain/`
- 実行中タスク: `docs/dev/tasks/active/`
- 完了タスク: `docs/dev/tasks/done/`
- 中止タスク: `docs/dev/tasks/cancelled/`
- 補助的な仕様・設定・入出力仕様: `docs/dev/reference/`

コードの実装内容だけを根拠に仕様を判断しないこと。
仕様変更がある場合は、コードより先に `docs/dev/` を更新すること。

---

## Read order
作業開始時は、次の順で読むこと。

1. `docs/dev/domain/`  
   用語、業務制約、前提条件を確認する。
2. `docs/dev/requirements/`  
   今回の変更が満たすべき要件を確認する。
3. `docs/dev/architecture/`  
   関連する構成、責務分割、依存関係を確認する。
4. `docs/dev/adr/`  
   過去の設計判断が今回の変更に影響するか確認する。
5. `docs/dev/tasks/active/`  
   今回実施対象のタスクと完了条件を確認する。

関連文書が見当たらない場合は、実装を進める前に不足文書を追加するか、最小限のドラフトを作成してから進めること。

---

## Working rules
- 大きな変更をいきなり実装しない。まず `docs/dev/tasks/active/` に作業単位を分解する。
- 1回の作業では、できるだけ小さい単位のタスクだけを進める。
- 1回の作業で扱うタスクは、実装と確認までをその場で完了できる粒度に分解する。1回で収まらない場合は、さらに分割してから着手する。
- 要件変更・設計変更が発生した場合は、コード変更より先に文書を更新する。
- 実装後は、関連する文書とテストを同期する。
- 完了したタスクは `docs/dev/tasks/done/` に移動する。
- 中止したタスクは `docs/dev/tasks/cancelled/` に移動し、理由を残す。

---

## Document policy

### Requirements
`docs/dev/requirements/` には「何を満たすべきか」を書く。
実装手順ではなく、目的・入出力・制約・完了条件を記述する。

最低限含める項目:
- 背景
- 目的
- スコープ
- 入力
- 出力
- 制約
- 完了条件

### Architecture
`docs/dev/architecture/` には「どう構成するか」を書く。
責務分離、モジュール構成、データフロー、画面構成、I/O境界などを記述する。

### ADR
`docs/dev/adr/` には、設計上の重要な判断と理由を残す。
後から「なぜそうしたか」を追えるようにする。

### Tasks
`docs/dev/tasks/active/` には、今回の作業計画を書く。
タスクは要件そのものではなく、実装・検証のための分解単位とする。

各タスクには最低限以下を含めること:
- 目的
- 対象ファイル
- 実施内容
- 完了条件
- 確認方法
- 関連要件/関連設計

---

## Task flow
新規開発または機能追加では、原則として次の流れで進めること。

1. 要求を確認する
2. `docs/dev/requirements/` に要件を書く、または更新する
3. 必要なら `docs/dev/architecture/` と `docs/dev/adr/` を更新する
4. `docs/dev/tasks/active/` に作業を分解する
5. タスク単位で実装する
6. テスト・確認を行う
7. 文書を同期する
8. 完了したタスクを `docs/dev/tasks/done/` に移動する

---

## Definition of done
作業完了とみなす条件は以下。

- 要件に対して実装が整合している
- 必要な設計文書が更新されている
- テストまたは確認手順が実施されている
- タスクファイルに結果と残課題が記録されている
- 完了タスクが `docs/dev/tasks/done/` に移動されている

---

## Do not
- `docs/dev/` を更新せずに仕様変更しない
- タスク分解なしで大規模変更をしない
- requirements に実装詳細を大量に書かない
- tasks を現在の仕様の正本として扱わない
- 完了後に task だけ閉じて requirements/architecture を放置しない

---

## Notes for directory-specific instructions
必要な場合のみ、サブディレクトリに追加の `AGENTS.md` を置いてよい。
ただし、局所ルールが本当に異なる場合だけに限定すること。

例:
- `src/gui/AGENTS.md` : GUI実装ルール
- `src/core/AGENTS.md` : コアロジックとI/O分離ルール
- `tests/AGENTS.md` : テスト命名・fixture方針

---

## Review guidelines

- 変更内容が `docs/dev/requirements/` `docs/dev/architecture/` `docs/dev/adr/` と矛盾していないか確認する。
- 仕様変更を伴うコード変更では、コードより先に関連文書が更新されているか確認する。
- `docs/dev/tasks/active/` に対応する作業計画がない大きな変更は指摘する。
- 変更に対して必要なテスト、確認手順、または手動確認方法が追加されているか確認する。
- 完了済みの変更なのに、関連タスクが `docs/dev/tasks/done/` に移動されていない場合は指摘する。
- requirements に実装詳細が入りすぎていないか確認する。
- architecture/adr を更新すべき変更なのに未更新なら指摘する。
- docs のタイポ、リンク切れ、用語ゆれも指摘する。Treat docs issues as P1.
- リファクタ時は責務分離の悪化、I/O境界の混濁、密結合の増加を指摘する。
- 変更が局所的なはずなのに、無関係な範囲へ波及している場合は指摘する。
