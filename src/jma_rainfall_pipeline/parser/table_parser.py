from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import date
import pandas as pd
from bs4 import BeautifulSoup, Tag

class TableParser(ABC):
    """テーブル構造を解析するための抽象基底クラス"""
    
    @abstractmethod
    def can_parse(self, table: Tag) -> bool:
        """このパーサーで処理可能なテーブルか判定"""
        pass
    
    @abstractmethod
    def parse_table(self, table: Tag, sample_date: date) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータのリストを返す"""
        pass
        
    def find_table(self, soup) -> Optional[Tag]:
        """HTMLから適切なテーブル要素を探して返す
        
        Args:
            soup: BeautifulSoupオブジェクト
            
        Returns:
            Optional[Tag]: 見つかったテーブル要素。見つからない場合はNone
        """
        # デフォルト実装: 最初のテーブルを返す
        table = soup.find('table')
        return table if table and self.can_parse(table) else None

class DataParser:
    """複数のテーブルパーサーを管理するクラス"""
    
    def __init__(self):
        self.parsers: List[TableParser] = []
    
    def add_parser(self, parser: TableParser) -> None:
        """パーサーを追加"""
        self.parsers.append(parser)
    
    def parse(self, html: str, sample_date: date) -> pd.DataFrame:
        """HTMLをパースしてDataFrameを返す
        
        Args:
            html: パース対象のHTML文字列
            sample_date: データのサンプル日付
            
        Returns:
            pd.DataFrame: パースされたデータ
            
        Raises:
            ValueError: サポートされていないテーブル形式の場合
        """
        soup = BeautifulSoup(html, "html.parser")
        
        # 各パーサーでテーブルを探す
        for parser in self.parsers:
            table = parser.find_table(soup)
            if table and parser.can_parse(table):
                data = parser.parse_table(table, sample_date)
                if data:  # 有効なデータがあれば返す
                    return pd.DataFrame(data)
        
        # デバグ用に利用可能なテーブル情報を収集
        tables = soup.find_all("table")
        available_tables = []
        for i, t in enumerate(tables):
            available_tables.append({
                "index": i,
                "classes": t.get('class', ['no-class']),
                "id": t.get('id', 'no-id'),
                "first_row": str(t.find('tr'))[:100] + '...' if t.find('tr') else 'no-rows'
            })
        
        raise ValueError(
            "サポートされていないテーブル形式です。\n"
            f"利用可能なテーブル: {available_tables}"
        )
