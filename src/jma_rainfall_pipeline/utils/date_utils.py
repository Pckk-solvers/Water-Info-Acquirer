"""
日付関連のユーティリティ関数を提供するモジュール
"""
from datetime import date, datetime
import calendar
import re

def parse_date_input(date_str: str) -> tuple[date, date]:
    """
    日付入力をパースして、開始日と終了日を返す
    
    Args:
        date_str: YYYY-MM-DD または YYYY-MM 形式の日付文字列
        
    Returns:
        tuple[date, date]: (開始日, 終了日)
        
    Raises:
        ValueError: 無効な日付形式の場合
    """
    # YYYY-MM 形式の場合
    if re.match(r'^\d{4}-\d{1,2}$', date_str):
        year, month = map(int, date_str.split('-'))
        if month < 1 or month > 12:
            raise ValueError("月は1から12の間で指定してください")
        
        # 月の最初の日と最後の日を取得
        _, last_day = calendar.monthrange(year, month)
        start_date = date(year, month, 1)
        end_date = date(year, month, last_day)
        return start_date, end_date
    
    # YYYY-MM-DD 形式の場合
    try:
        input_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        return input_date, input_date
    except ValueError:
        raise ValueError("無効な日付形式です。YYYY-MM-DD または YYYY-MM 形式で入力してください")

def validate_date_range(start_date: str, end_date: str) -> tuple[date, date]:
    """
    開始日と終了日を検証し、dateオブジェクトとして返す
    
    Args:
        start_date: 開始日 (YYYY-MM-DD または YYYY-MM)
        end_date: 終了日 (YYYY-MM-DD または YYYY-MM)
        
    Returns:
        tuple[date, date]: (開始日, 終了日)
        
    Raises:
        ValueError: 無効な日付範囲の場合
    """
    start, _ = parse_date_input(start_date)
    _, end = parse_date_input(end_date)
    
    if start > end:
        raise ValueError("開始日は終了日より前の日付を指定してください")
    
    return start, end
