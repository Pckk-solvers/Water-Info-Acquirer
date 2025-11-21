# src/jma_rainfall_pipeline/parser/date_utils.py
from datetime import date
from typing import Optional
from bs4 import BeautifulSoup
import re

def extract_date_from_html(html_content: str) -> date:
    """HTMLから日付を抽出する
    
    Args:
        html_content: HTML文字列
        
    Returns:
        date: 抽出した日付
        
    Raises:
        ValueError: 日付が見つからないか、不正な形式の場合
        
    Examples:
        >>> html = '<h3>稚内（宗谷地方) 2025年7月1日（１０分ごとの値）</h3>'
        >>> extract_date_from_html(html)
        datetime.date(2025, 7, 1)
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    h3 = soup.find('h3')
    if not h3:
        raise ValueError("日付を含むh3タグが見つかりません")
    
    # 日付部分を抽出（例：'2025年7月1日' または '2025年6月'）
    date_match = re.search(
        r'(\d{4})年(\d{1,2})月(?:(\d{1,2})日)?',
        h3.text
    )
    
    if not date_match:
        raise ValueError("日付の形式が正しくありません")
    
    year = int(date_match.group(1))
    month = int(date_match.group(2))
    day = int(date_match.group(3)) if date_match.group(3) else 1  # 日が省略されている場合は1日とする
    
    try:
        return date(year, month, day)
    except ValueError as e:
        raise ValueError(f"無効な日付です: {year}年{month}月{day}日") from e
    
def format_date(d: date) -> str:
    """日付を「YYYY/MM/DD」形式の文字列にフォーマットする
    
    Args:
        d: フォーマットする日付
        
    Returns:
        str: フォーマットされた日付文字列
    """
    return d.strftime("%Y/%m/%d")