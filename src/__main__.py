"""
水文データ取得ツール - CLIエントリーポイント

新しい統合APIを使用したメインエントリーポイント
"""

import argparse
import sys
from pathlib import Path

# 統一ログシステムの設定
from .wia.logging_config import setup_logging, get_logger
from .wia.exception_handler import create_cli_exception_handler

# ログシステム初期化
setup_logging(log_level="INFO", log_file="water_info_acquirer.log")
logger = get_logger(__name__)

# CLI用例外ハンドラー
cli_exception_handler = create_cli_exception_handler()


def show_error(message: str):
    """
    CLIでのエラー表示（後方互換性のため残す）
    
    Args:
        message: エラーメッセージ
    """
    print(f"エラー: {message}", file=sys.stderr)
    logger.error(f"CLIエラー: {message}")


def main():
    """メイン処理"""
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
    
    logger.info(f"水文データ取得ツール開始 (single_sheet={single_sheet_mode})")

    try:
        # 新しいGUIクラスを使用
        from .wia.gui import WWRApp
        app = WWRApp(single_sheet_mode=single_sheet_mode)
        app.run()
        logger.info("アプリケーション正常終了")
        
    except ImportError as e:
        # インポートエラーは統一例外ハンドリング（要件4.3、4.5対応）
        context = "モジュールインポート"
        cli_exception_handler.handle_exception(e, context, exit_on_error=True)
        
    except Exception as e:
        # 想定外エラーは統一例外ハンドリング（要件4.4、4.5対応）
        context = "アプリケーション起動"
        cli_exception_handler.handle_exception(e, context, exit_on_error=True)


if __name__ == '__main__':
    main()
