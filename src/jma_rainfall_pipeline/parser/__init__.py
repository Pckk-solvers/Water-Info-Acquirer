# jma_rainfall_pipeline/parser/__init__.py
from datetime import date
from typing import Optional, Union
import pandas as pd

# 新しいパーサーインターフェース
from .table_parser import DataParser, TableParser
from .daily_table_parser import create_daily_parser
from .hourly_table_parser import create_hourly_parser
from .minute10_table_parser import Minute10TableParser

def get_parser(freq: str, obs_type: str = 'a1') -> TableParser:
    """データ種別に応じたパーサーを返すファクトリ関数
    
    Args:
        freq: データ種別 ('daily', 'hourly', '10min')
        obs_type: 観測所タイプ ('a1' or 's1')
        
    Returns:
        TableParser: 指定されたデータ種別のパーサーインスタンス
    """
    if freq == 'hourly':
        return create_hourly_parser(obs_type)
    
    parsers = {
        'daily': create_daily_parser(obs_type),
        '10min': Minute10TableParser(),
    }
    
    if freq not in parsers:
        raise ValueError(f"サポートされていないデータ種別です: {freq}")
        
    return parsers[freq]

def parse_html(html: str, freq: str, sample_date: date, obs_type: str = 'a1') -> pd.DataFrame:
    """HTMLをパースしてDataFrameを返すヘルパー関数
    
    Args:
        html: パース対象のHTML文字列
        freq: データ種別 ('daily', 'hourly', '10min')
        sample_date: サンプル日付（日付が明示されていない場合に使用）
        obs_type: 観測所タイプ ('a1' or 's1')
        
    Returns:
        pd.DataFrame: パースされたデータ
    """
    parser = DataParser()
    parser.add_parser(get_parser(freq, obs_type))
    return parser.parse(html, sample_date)

# 後方互換性のためのエイリアス
def parse_precipitation_table(html: str, sample_date: date) -> pd.DataFrame:
    """後方互換性のための関数（既存コードとの互換性を保つ）
    
    Note: この関数は互換性のために残されています。
    新しいコードでは parse_html() を直接使用してください。
    """
    return parse_html(html, 'daily', sample_date)
