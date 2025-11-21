"""バージョン情報管理モジュール"""

# バージョン情報
__version__ = "0.3.4"
__version_info__ = (0, 3, 4)

# アプリケーション情報
__app_name__ = "JMA Rainfall Pipeline"
__description__ = "気象庁の降水量データを取得・エクスポートするツール"
__author__ = "JMA Rainfall Pipeline Team"
__copyright__ = "2025"

def get_version():
    """バージョン文字列を取得する"""
    return __version__

def get_version_info():
    """バージョン情報タプルを取得する"""
    return __version_info__

def get_app_info():
    """アプリケーション情報を取得する"""
    return {
        "name": __app_name__,
        "version": __version__,
        "description": __description__,
        "author": __author__,
        "copyright": __copyright__
    }

def get_full_title():
    """完全なアプリケーションタイトルを取得する"""
    return f"{__app_name__} v{__version__}"

def get_version_string():
    """詳細なバージョン情報文字列を取得する"""
    return f"{__app_name__} v{__version__} ({__copyright__})"
