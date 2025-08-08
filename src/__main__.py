import argparse
from src.main_datetime import WWRApp, show_error

if __name__ == '__main__':
    # コマンドライン引数のパーサー設定
    parser = argparse.ArgumentParser(description='水文データ取得ツール')
    parser.add_argument(
        '--single-sheet',
        action='store_true',
        help='1シート目に全データを出力してチャートを挿入（デフォルトは年ごと分割）'
    )
    args = parser.parse_args()

    # フラグを変数に格納
    single_sheet_mode = args.single_sheet

    try:
        # WWRAppに引数を渡す（要対応）
        WWRApp(single_sheet_mode=single_sheet_mode)
    except Exception as e:
        show_error(str(e))
