import re
from abc import abstractmethod
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple
from bs4 import Tag, BeautifulSoup
from .date_utils import extract_date_from_html
from .table_parser import TableParser
from jma_rainfall_pipeline.logger.app_logger import get_logger

logger = get_logger(__name__)

class DailyTableParser(TableParser):
    """日次データ用テーブルパーサーのベースクラス"""
    
    def can_parse(self, table: Tag) -> bool:
        """日次データのテーブルかどうかを判定
        
        Returns:
            bool: テーブルが有効な形式であればTrue、それ以外はFalse
        """
        # 1. 基本構造の確認
        if not table:
            return False
            
        rows = table.find_all('tr', recursive=True)
        if not rows:
            return False

        # 2. ヘッダー行の確認（1行目のみ確認）
        first_row_headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
        
        # 必須カラム（日付、降水量、気温）
        # S1形式: ['日', '気圧(hPa)', '降水量(mm)', '気温(℃)', ...]
        # A1形式: ['日', '降水量', '気温', ...]
        header_text = ' '.join(first_row_headers)
        
        # ヘッダーのバリエーションを考慮してチェック
        has_date = '日' in header_text
        has_precipitation = any(x in header_text for x in ['降水量(mm)', '降水量'])
        has_temperature = any(x in header_text for x in ['気温(℃)', '気温'])
        
        if not all([has_date, has_precipitation, has_temperature]):
            missing = []
            if not has_date: missing.append('日')
            if not has_precipitation: missing.append('降水量(mm) or 降水量')
            if not has_temperature: missing.append('気温(℃) or 気温')
            logger.debug(f"必須ヘッダーが見つかりません。不足しているヘッダー: {missing}")
            return False

        # 3. データ行の確認（ヘッダー行の直後は単位行の可能性があるため、複数行を確認）
        if len(rows) < 2:  # ヘッダー行 + データ行（最低1行）
            return False

        # 4. データ型の簡易チェック
        # データ行の開始位置を特定（ヘッダー行の直後が単位行かどうかで変わる）
        data_row_start = 1  # ヘッダー行の次
        max_checks = min(5, len(rows))  # 最大5行までチェック
        
        # データ行を検出
        for i in range(1, max_checks):
            row = rows[i].find_all(['td', 'th'])
            if not row:
                continue
                
            # 行のテキストを取得
            row_text = ' '.join([cell.get_text(strip=True) for cell in row])
            
            # 日付セルをチェック（1列目）
            date_cell = row[0].get_text(strip=True)
            
            # 日付形式のチェック（数値のみ or 日付形式 or 日本語の文字列）
            if re.search(r'^\d{1,2}(?:/\d{1,2})?$|^[\d.]+$|^[\u4e00-\u9fff]+$', date_cell):
                data_row_start = i
                break
                
            # 単位行とみなす条件: 数値や日付でない文字列が含まれている
            if any(unit in row_text for unit in ['(mm)', '(℃)', '(hPa)', '(m/s)', '風速', '平均', '合計']):
                data_row_start = i + 1
        else:
            return False

        # データ行が存在するか確認
        if len(rows) <= data_row_start:
            return False

        try:
            # データ行を取得
            first_data_row = rows[data_row_start].find_all(['td', 'th'])
            if not first_data_row:
                return False
                
            # 日付カラムのチェック（1列目）
            date_cell = first_data_row[0].get_text(strip=True)
            
            # 日付形式の最終チェック
            if not re.search(r'^\d{1,2}(?:/\d{1,2})?$|^[\d.]+$|^[\u4e00-\u9fff]+$', date_cell):
                return False
                
        except (IndexError, AttributeError) as e:
            logger.debug(f"テーブル検証エラー: {e}")
            return False

        return True
    
    def find_table(self, soup) -> Optional[Tag]:
        """日次データのテーブルを探して返す"""
        selectors = self._get_table_selectors()
        for selector in selectors:
            try:
                table = soup.select_one(selector)
                if table and self.can_parse(table):
                    return table
            except Exception as e:
                logger.debug(f"テーブルセレクター '{selector}' でエラー: {e}")
        return super().find_table(soup)
    
    def _get_sample_date(self, sample_date: Optional[date], html_content: Optional[str]) -> date:
        """sample_date を取得する。指定されていない場合は html_content から抽出する"""
        if sample_date is not None:
            return sample_date
            
        if html_content is None:
            raise ValueError("sample_date または html_content のいずれかは必須です")
            
        sample_date = extract_date_from_html(html_content)
        return sample_date

    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータを返す
        
        Args:
            table: BeautifulSoupのテーブルオブジェクト
            sample_date: 対象日付（省略時はhtml_contentから自動抽出）
            html_content: HTML文字列（sample_dateがNoneの場合に使用）
            
        Returns:
            パース結果のリスト
            
        Raises:
            ValueError: 必須の引数が不足している場合
        """
        logger.info("Starting daily table parsing")
        # 日付を取得
        sample_date = self._get_sample_date(sample_date, html_content)
        
        all_rows = table.find_all('tr')
        header_row_idx = self._find_header_row(all_rows)
        data_rows = all_rows[header_row_idx + 1:]
        
        data = []
        current_year = sample_date.year
        current_month = sample_date.month
        
        for row in data_rows:
            row_data = self._parse_row(row, current_year, current_month)
            if row_data:
                data.append(row_data)
        
        logger.info(f"Daily table parsing completed successfully. Parsed {len(data)} records.")
        return data
    
    def _find_header_row(self, rows: List[Tag]) -> int:
        """ヘッダー行のインデックスを返す"""
        for i, row in enumerate(rows):
            if row.find('th', string=lambda x: x and '日' in str(x)):
                return i
        raise ValueError("ヘッダー行が見つかりません")
    
    @abstractmethod
    def _get_table_selectors(self) -> List[str]:
        """テーブルを特定するためのセレクタを返す（サブクラスで実装）"""
        pass
    
    @abstractmethod
    def _parse_row(self, row: Tag, year: int, month: int) -> Optional[Dict[str, Any]]:
        """行データをパースする（サブクラスで実装）"""
        pass
    
    def _parse_day(self, day_str: str) -> int:
        """日付文字列から日を抽出"""
        match = re.search(r"(\d+)", day_str)
        return int(match.group(1)) if match else None
    
    def _parse_float(self, value: str) -> Optional[float]:
        """文字列をfloatに変換（欠測値はNoneを返す）"""
        if not value or value.strip() in ('-', '--', '///', '×'):
            return None
        try:
            # カンマを削除してからパース（例：1,234.5 → 1234.5）
            return float(value.replace(',', ''))
        except ValueError:
            return None 

class DailyTableParserA1(DailyTableParser):
    """アメダス観測所（a1）用日次データパーサー"""
    
    def _get_table_selectors(self) -> List[str]:
        return [
            'table#tablefix1.data2_s',
            'table.data2_s',
            'table:has(tr:has(th:contains("日")))'
        ]
        
    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータを返す
        
        Args:
            table: BeautifulSoupのテーブルオブジェクト
            sample_date: 対象日付（省略時はhtml_contentから自動抽出）
            html_content: HTML文字列（sample_dateがNoneの場合に使用）
            
        Returns:
            パース結果のリスト
        """
        # 親クラスのメソッドを呼び出して日付の処理を行う
        sample_date = self._get_sample_date(sample_date, html_content)
        
        all_rows = table.find_all('tr')
        header_row_idx = self._find_header_row(all_rows)
        data_rows = all_rows[header_row_idx + 1:]
        
        # 年月を取得（ヘッダー行の最初のセルから）
        year = sample_date.year
        month = sample_date.month
        
        data = []
        for row in data_rows:
            row_data = self._parse_row(row, year, month)
            if row_data:
                data.append(row_data)
                
        return data
    
    def _parse_row(self, row: Tag, year: int, month: int) -> Optional[Dict[str, Any]]:
        cols = [td.get_text(strip=True) for td in row.find_all(['th', 'td'])]
        if not cols or len(cols) < 18:  # 最低限必要な列数
            return None
            
        day = self._parse_day(cols[0])
        if not day:
            return None
            
        try:
            return {
                'date': datetime(year, month, day).date(),
                # 降水量関連
                'precipitation_total': self._parse_float(cols[1]),  # 降水量 合計 (mm)
                'precipitation_max_1h': self._parse_float(cols[2]),  # 降水量 最大1時間 (mm)
                'precipitation_max_10m': self._parse_float(cols[3]),  # 降水量 最大10分間 (mm)
                # 気温関連
                'temperature_avg': self._parse_float(cols[4]),  # 平均気温 (℃)
                'temperature_max': self._parse_float(cols[5]),  # 最高気温 (℃)
                'temperature_min': self._parse_float(cols[6]),  # 最低気温 (℃)
                # 湿度関連
                'humidity_avg': self._parse_float(cols[7]),     # 平均湿度 (%)
                'humidity_min': self._parse_float(cols[8]),     # 最小湿度 (%)
                # 風関連
                'wind_speed_avg': self._parse_float(cols[9]),   # 平均風速 (m/s)
                'wind_speed_max': self._parse_float(cols[10]),  # 最大風速 (m/s)
                'wind_direction_max': cols[11].strip() or None,  # 最大風速の風向
                'wind_gust': self._parse_float(cols[12]),       # 最大瞬間風速 (m/s)
                'wind_gust_direction': cols[13].strip() or None,  # 最大瞬間風速の風向
                'wind_direction_most': cols[14].strip() or None,  # 最多風向
                # 日照・雪関連
                'sunshine_hours': self._parse_float(cols[15]),  # 日照時間 (h)
                'snow_fall': self._parse_float(cols[16]),       # 降雪の深さの合計 (cm)
                'snow_depth': self._parse_float(cols[17]) if len(cols) > 17 else None,  # 最深積雪 (cm)
                'raw_data': '|'.join(cols)  # デバッグ用に生データも保存
            }
        except (ValueError, IndexError) as e:
            print(f"行のパース中にエラーが発生しました: {e}")
            print(f"行データ: {cols}")
            return None

class DailyTableParserS1(DailyTableParser):
    """気象台・測候所（s1）用日次データパーサー"""
    
    def _get_table_selectors(self) -> List[str]:
        return [
            'table#tablefix1.data2_s',
            'table.data2_s',
            'table:has(tr:has(th:contains("日")))',
            'table[summary*="日ごとの値"]'
        ]
    
    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータを返す
        
        Args:
            table: BeautifulSoupのテーブルオブジェクト
            sample_date: 対象日付（省略時はhtml_contentから自動抽出）
            html_content: HTML文字列（sample_dateがNoneの場合に使用）
            
        Returns:
            パース結果のリスト
        """
        # 親クラスのメソッドを呼び出して日付の処理を行う
        sample_date = self._get_sample_date(sample_date, html_content)
        
        all_rows = table.find_all('tr')
        header_row_idx = self._find_header_row(all_rows)
        data_rows = all_rows[header_row_idx + 1:]
        
        # 年月を取得（ヘッダー行の最初のセルから）
        year = sample_date.year
        month = sample_date.month
        
        data = []
        for row in data_rows:
            row_data = self._parse_row(row, year, month)
            if row_data:
                data.append(row_data)
                
        return data
    
    def _parse_row(self, row: Tag, year: int, month: int) -> Optional[Dict[str, Any]]:
        cols = [td.get_text(strip=True) for td in row.find_all(['th', 'td'])]
        if not cols or len(cols) < 20:  # 最低限必要な列数
            return None
            
        day = self._parse_day(cols[0])
        if not day:
            return None
            
        try:
            return {
                'date': datetime(year, month, day).date(),
                'pressure_ground': self._parse_float(cols[1]),  # 現地気圧 (hPa)
                'pressure_sea': self._parse_float(cols[2]),     # 海面気圧 (hPa)
                'precipitation_total': self._parse_float(cols[3]),  # 降水量 合計 (mm)
                'precipitation_max_1h': self._parse_float(cols[4]),  # 降水量 最大1時間 (mm)
                'precipitation_max_10m': self._parse_float(cols[5]),  # 降水量 最大10分間 (mm)
                'temperature_avg': self._parse_float(cols[6]),  # 平均気温 (℃)
                'temperature_max': self._parse_float(cols[7]),  # 最高気温 (℃)
                'temperature_min': self._parse_float(cols[8]),  # 最低気温 (℃)
                'humidity_avg': self._parse_float(cols[9]),     # 平均湿度 (%)
                'humidity_min': self._parse_float(cols[10]),    # 最小湿度 (%)
                'wind_speed_avg': self._parse_float(cols[11]),  # 平均風速 (m/s)
                'wind_speed_max': self._parse_float(cols[12]),  # 最大風速 (m/s)
                'wind_direction_max': cols[13].strip() or None,  # 最大風速の風向
                'wind_gust': self._parse_float(cols[14]),       # 最大瞬間風速 (m/s)
                'wind_gust_direction': cols[15].strip() or None,  # 最大瞬間風速の風向
                'sunshine_hours': self._parse_float(cols[16]),  # 日照時間 (h)
                'snow_fall': self._parse_float(cols[17]) if cols[17] != '--' else None,  # 降雪 (cm)
                'snow_depth': self._parse_float(cols[18]) if cols[18] != '--' else None,  # 最深積雪 (cm)
                'weather_day': cols[19].strip() or None,        # 天気概況（昼）
                'weather_night': cols[20].strip() if len(cols) > 20 else None,  # 天気概況（夜）
                'raw_data': '|'.join(cols)  # デバッグ用に生データも保存
            }
        except (ValueError, IndexError) as e:
            print(f"行のパース中にエラーが発生しました: {e}")
            print(f"行データ: {cols}")
            return None

def create_daily_parser(obs_type: str) -> DailyTableParser:
    """観測タイプに応じた日次データパーサーを作成
    
    Args:
        obs_type: 観測所タイプ ('a1' または 's1')
        
    Returns:
        DailyTableParser: 日次データパーサー
        
    Raises:
        ValueError: サポートされていない観測所タイプが指定された場合
    """
    if obs_type == 'a1':
        return DailyTableParserA1()
    elif obs_type == 's1':
        return DailyTableParserS1()
    else:
        raise ValueError(f"サポートされていない観測所タイプです: {obs_type}")