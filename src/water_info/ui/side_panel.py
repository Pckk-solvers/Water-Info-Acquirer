"""Side panel (help text) builder for water_info UI."""

from __future__ import annotations

from tkinter import Label, LEFT


def populate_side_panel(parent) -> None:
    desc_labels = []
    sections = [
        ("- ツールの説明 -", "本ツールは「国土交通省・水文水質データベース」で公開されている水位・流量・雨量データを取得するツールです。"
                            "取得したデータはExcel形式（1観測所1ファイル）で出力されます。"
                            "複数年分のデータをまとめて取得した際は、取得した年数分のシートが作成されます。", "black"),
        ("・観測所記号入力欄", "半角数字でコードを入力し、[追加]をクリックしてください。", "black"),
        ("・「追加／削除」", "追加ボタンをクリック、または「Enterキー」を押すと、「観測所記号入力欄」に入力した観測所記号が「データ取得観測所一覧」へ追加されます。"
                            "「データ取得観測所一覧」から観測所を選択し削除ボタンをクリックすると、「データ取得観測所一覧」から選択した観測所を削除することができます。", "black"),
        ("・データ取得観測所一覧", "ここに表示されている観測所のデータが取得されます。", "black"),
        ("・取得項目", "水位・流量・雨量の中から、データを取得したい項目を選択してください。\n※1項目のみ選択可能", "black"),
        ("・取得期間", "データを取得したい期間の開始年月と終了年月を入力してください。", "black"),
        ("・日データ", "時刻データではなく、日データを取得したい場合に、チェックを入れてください。", "black"),
        ("・指定全期間シート挿入", "年ごとのシートに加えて、全期間のデータを1シートにまとめたものを作成したい場合に、チェックを入れてください。", "black"),
        ("・注意事項", "指定した取得期間内に有効なデータが1件も存在しない場合は、下記エラーメッセージが表示され、該当観測所のExcelファイルは出力されません。"
         "\n「指定期間に有効なデータが見つかりませんでした。」"
         "\nまた、エラー時は”OK”ボタンを押すまで画面操作が行えなくなるため、エラーメッセージを確認後、”OK”ボタンをクリックしてください。"
         "\n[Error 13] こちらはExcelが開かれていて書き込みができない状態です。", "red")
    ]
    for title, text, color in sections:
        Label(parent, text=title, bg="#eef6f9", fg=color, font=(None, 10, 'bold')).pack(anchor='w', pady=(8, 0), padx=5)
        lbl = Label(parent, text=text, bg="#eef6f9", fg=color, justify=LEFT, wraplength=1)
        lbl.pack(anchor='w', padx=5)
        desc_labels.append(lbl)

    def on_configure(event):
        new_wrap = event.width - 10
        for lbl in desc_labels:
            lbl.configure(wraplength=new_wrap)

    parent.bind('<Configure>', on_configure)
