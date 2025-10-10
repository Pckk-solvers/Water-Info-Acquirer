"""
例外クラス定義

業務例外とシステム例外の定義
"""


class WaterInfoAcquirerError(Exception):
    """基底例外クラス"""
    pass


class EmptyDataError(WaterInfoAcquirerError):
    """データが空の場合の例外"""
    
    def __init__(self, message: str = "取得したデータが空です"):
        self.message = message
        super().__init__(self.message)


class NetworkError(WaterInfoAcquirerError):
    """ネットワークエラー"""
    
    def __init__(self, message: str = "ネットワーク接続エラーが発生しました"):
        self.message = message
        super().__init__(self.message)


class ParseError(WaterInfoAcquirerError):
    """HTML解析エラー"""
    
    def __init__(self, message: str = "データの解析に失敗しました"):
        self.message = message
        super().__init__(self.message)


# 既存のEmptyExcelWarningとの互換性のため
class EmptyExcelWarning(EmptyDataError):
    """既存コードとの互換性のための例外クラス"""
    pass


def is_business_exception(error: Exception) -> bool:
    """
    業務例外かどうかを判定
    
    Args:
        error: 例外オブジェクト
        
    Returns:
        bool: 業務例外の場合True
    """
    return isinstance(error, (EmptyDataError, EmptyExcelWarning))


def is_system_exception(error: Exception) -> bool:
    """
    システム例外かどうかを判定
    
    Args:
        error: 例外オブジェクト
        
    Returns:
        bool: システム例外の場合True
    """
    return isinstance(error, (NetworkError, ParseError))


def get_user_friendly_message(error: Exception) -> str:
    """
    ユーザー向けのエラーメッセージを取得（要件4.4対応）
    
    Args:
        error: 例外オブジェクト
        
    Returns:
        str: ユーザー向けメッセージ
    """
    if is_business_exception(error):
        # 業務例外：そのままのメッセージを表示
        return str(error)
    elif is_system_exception(error):
        # システム例外：そのままのメッセージを表示
        return str(error)
    elif isinstance(error, WaterInfoAcquirerError):
        # アプリケーション例外：そのままのメッセージを表示
        return str(error)
    else:
        # 想定外例外の場合は短文通知（要件4.4）
        return "想定外のエラーが発生しました。ログファイルを確認してください。"


def get_exit_code(error: Exception) -> int:
    """
    例外に対応する終了コードを取得
    
    Args:
        error: 例外オブジェクト
        
    Returns:
        int: 終了コード
    """
    if is_business_exception(error):
        return 2  # 業務例外
    elif is_system_exception(error):
        return 3  # システム例外
    elif isinstance(error, WaterInfoAcquirerError):
        return 4  # アプリケーション例外
    else:
        return 1  # 想定外例外