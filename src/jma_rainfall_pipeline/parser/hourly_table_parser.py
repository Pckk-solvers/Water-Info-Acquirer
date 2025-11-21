import re
from datetime import datetime, date, time
from typing import List, Dict, Any, Optional
from bs4 import Tag
from .date_utils import extract_date_from_html
from jma_rainfall_pipeline.logger.app_logger import get_logger
from jma_rainfall_pipeline.parser.table_parser import TableParser

logger = get_logger(__name__)

class BaseHourlyTableParser(TableParser):
    """時間別データ用ベースパーサー"""
    
    def _get_table_selectors(self) -> List[str]:
        """テーブルを特定するためのCSSセレクタのリストを返す"""
        return [
            'table#tablefix1.data2_s',  # IDとクラスで特定
            'table.data2_s',  # クラス名で特定
            'table.data',  # 一般的なデータテーブル
            'table[summary*="時"]',  # 時刻を含むテーブル
            'table'  # フォールバック
        ]
        
    def find_table(self, soup) -> Optional[Tag]:
        """時間別データのテーブルを探して返す"""
        selectors = self._get_table_selectors()
        for selector in selectors:
            try:
                table = soup.select_one(selector)
                if table and self.can_parse(table):
                    return table
            except Exception as e:
                logger.debug(f"テーブルセレクター '{selector}' でエラー: {e}")
                continue
        return super().find_table(soup)
        
    def can_parse(self, table: Tag) -> bool:
        """テーブルがパース可能な形式か検証する"""
        # 1. 基本構造の確認
        if not table:
            return False
            
        rows = table.find_all('tr', recursive=True)
        if not rows:
            return False

        # 2. ヘッダー行の確認（1行目のみ確認）
        first_row_headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
        
        required_headers = ['時', '降水量(mm)', '気温(℃)']  # 必須カラム
        missing_headers = [h for h in required_headers if h not in ' '.join(first_row_headers)]
        if missing_headers:
            return False

        # 3. データ行の確認（3行目以降を確認）
        if len(rows) < 3:  # ヘッダー2行 + 最低1データ行
            return False

        # 4. データ型の簡易チェック（オプション）
        try:
            # 3行目（インデックス2）を最初のデータ行として確認
            first_data_row = rows[2].find_all(['td', 'th'])
            if not first_data_row:
                return False
                
            # 時刻カラムのチェック（1列目）
            time_cell = first_data_row[0].get_text(strip=True)
            
            # 数値のみ（時）または「時:分」形式を許容
            if not re.search(r'^\d{1,2}(?::\d{1,2})?$', time_cell):
                return False
                
        except (IndexError, AttributeError) as e:
            logger.debug(f"テーブル検証エラー: {e}")
            return False

        return True
    
    def _parse_hour(self, hour_str: str) -> Optional[int]:
        """時間文字列をパースして時間を返す"""
        match = re.search(r"(\d+)", hour_str)
        return int(match.group(1)) if match else None
    
    def _parse_datetime(self, date_obj: date, hour: int) -> datetime:
        """日付と時間からdatetimeオブジェクトを作成
        
        JMAの時間データは1-24時で1日を表すため、24時はその日の23:59:59.999999として扱う
        例：6月2日1時 〜 6月3日0時（24時）は全て6月2日として扱う
        """
        # 24時はその日の23:59:59.999999として扱う
        if hour == 24:
            return datetime.combine(date_obj, time(23, 59, 59, 999999))
        # 24時を超える場合はエラー（通常は発生しない）
        elif hour > 24:
            raise ValueError(f"不正な時間です: {hour}時")
        return datetime.combine(date_obj, time(hour=hour))
    
    def _parse_weather_icon(self, td: Tag) -> Optional[str]:
        """天気アイコンから天気コードを取得"""
        img = td.find('img')
        if img and 'alt' in img.attrs:
            return img['alt']
        return None
    
    def _parse_snow(self, value: str) -> Optional[float]:
        """雪関連の値をパース"""
        if not value or value == '×' or value == '///':
            return None
        return self._parse_float(value)
    
    def _get_sample_date(self, sample_date: Optional[date], html_content: Optional[str]) -> date:
        """sample_date を取得する。指定されていない場合は html_content から抽出する"""
        if sample_date is not None:
            return sample_date
            
        if html_content is None:
            raise ValueError("sample_date または html_content のいずれかは必須です")
            
        sample_date = extract_date_from_html(html_content)
        return sample_date
        
    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータを抽出する
        
        Args:
            table: BeautifulSoupのテーブルオブジェクト
            sample_date: 対象日付（省略時はhtml_contentから自動抽出）
            html_content: HTML文字列（sample_dateがNoneの場合に使用）
            
        Returns:
            パース結果のリスト
            
        Raises:
            ValueError: 必須の引数が不足している場合
        """
        logger.info("Starting hourly table parsing")
        # サブクラスで実装する
        raise NotImplementedError("Subclasses must implement parse_table")
        
    def _parse_float(self, value: str) -> Optional[float]:
        """文字列をfloatに変換（欠測値はNoneを返す）"""
        if not value or value.strip() in ('-', '--', '///', '×'):
            return None
        try:
            # カンマを削除してからパース（例：1,234.5 → 1234.5）
            return float(value.replace(',', '').strip())
        except (ValueError, TypeError):
            return None


class HourlyTableParserA1(BaseHourlyTableParser):
    """アメダス（A1）用の時間別データパーサー"""
    
    # カラムマッピング（列インデックス → フィールド名）
    COLUMN_MAPPING = {
        0: 'hour',               # 時
        1: 'precipitation',      # 降水量 (mm)
        2: 'temperature',        # 気温 (℃)
        3: 'dew_point',          # 露点温度 (℃)
        4: 'vapor_pressure',     # 蒸気圧 (hPa)
        5: 'humidity',           # 湿度 (%)
        6: 'wind_speed',         # 平均風速 (m/s)
        7: 'wind_direction',     # 風向
        8: 'sunshine_hours',     # 日照時間 (h)
        9: 'snow_fall',          # 降雪 (cm)
        10: 'snow_depth'         # 積雪 (cm)
    }
    
    def parse_table(self, table: Tag, sample_date: Optional[date] = None, 
                   html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """テーブルをパースしてデータを返す
        
        Args:
            table: BeautifulSoupのTableオブジェクト
            sample_date: サンプル日付（指定がない場合はHTMLから解析）
            html_content: 完全なHTMLコンテンツ（日付解析用）
            
        Returns:
            気象データのリスト。各データは辞書形式
        """
        logger.info("Starting A1 hourly table parsing")
        sample_date = self._get_sample_date(sample_date, html_content)
        all_rows = table.find_all('tr')
        data = []
        current_date = sample_date
        
        # ヘッダー行をスキップ（最初の2行）
        data_rows = all_rows[2:] if len(all_rows) > 2 else all_rows
        
        for row in data_rows:
            cols = row.find_all(['th', 'td'])
            if not cols or len(cols) < 11:  # 最低11列必要
                continue
                
            # 日付行の処理（例: "1日"）
            date_match = re.search(r"(\d+)日", cols[0].get_text(strip=True))
            if date_match:
                try:
                    day = int(date_match.group(1))
                    current_date = date(sample_date.year, sample_date.month, day)
                except (ValueError, IndexError) as e:
                    logger.warning(f"日付の解析に失敗しました: {cols[0].get_text()}, {e}")
                continue
                
            # 時間データの処理
            hour = self._parse_hour(cols[0].get_text(strip=True))
            if hour is None:
                continue
                
            try:
                timestamp = self._parse_datetime(current_date, hour)
                row_data = {
                    'date': timestamp.date(),
                    'time': timestamp.time(),
                    'datetime': timestamp,
                }
                
                # 各カラムをマッピングに従って処理
                for col_idx, field_name in self.COLUMN_MAPPING.items():
                    if col_idx >= len(cols):
                        continue
                        
                    value = cols[col_idx].get_text(strip=True)
                    
                    # 値の型変換
                    if field_name == 'precipitation':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'temperature':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'dew_point':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'vapor_pressure':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'humidity':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'wind_speed':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name == 'wind_direction':
                        row_data[field_name] = value if value and value != '///' else None
                    elif field_name == 'sunshine_hours':
                        row_data[field_name] = self._parse_float(value)
                    elif field_name in ['snow_fall', 'snow_depth']:
                        row_data[field_name] = self._parse_snow(value)
                    else:
                        row_data[field_name] = value
                
                # 生データも保存（デバッグ用）
                row_data['raw_data'] = '|'.join(td.get_text(strip=True) for td in cols)
                data.append(row_data)
                
            except Exception as e:
                logger.warning(f"行の解析中にエラーが発生しました: {str(e)}\n行データ: {[td.get_text(strip=True) for td in cols]}")
                continue
                
        logger.info(f"A1 hourly table parsing completed successfully. Parsed {len(data)} records.")
        return data


class HourlyTableParserS1(BaseHourlyTableParser):
    """気象台（S1）用の時間別データパーサー"""
    
    # 列のインデックスとフィールド名のマッピング
    COLUMN_MAPPING = {
        0: 'hour',
        1: 'pressure_ground',    # 現地気圧
        2: 'pressure_sea',       # 海面気圧
        3: 'precipitation',      # 降水量
        4: 'temperature',        # 気温
        5: 'dew_point',          # 露点温度
        6: 'vapor_pressure',     # 蒸気圧
        7: 'humidity',           # 湿度
        8: 'wind_speed',         # 風速
        9: 'wind_direction',     # 風向
        10: 'sunshine_hours',    # 日照時間
        11: 'solar_radiation',   # 全天日射量
        12: 'snow_fall',         # 降雪
        13: 'snow_depth',        # 積雪
        14: 'weather',           # 天気
        15: 'cloud_cover',       # 雲量
        16: 'visibility'         # 視程
    }
    
    def _extract_headers(self, header_row: Tag) -> List[str]:
        """ヘッダー行から列名を抽出して返す"""
        headers = []
        for th in header_row.find_all(['th', 'td']):
            header_text = th.get_text(separator=' ', strip=True)
            header_text = ' '.join(header_text.split())
            headers.append(header_text)
        return headers

    def _map_jp_to_en_headers(self, jp_headers: List[str]) -> Dict[int, str]:
        """ヘッダー行の解析（S1形式では固定のマッピングを使用）"""
        return self.COLUMN_MAPPING

    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        # 日付を取得
        logger.info("Starting S1 hourly table parsing")
        sample_date = self._get_sample_date(sample_date, html_content)
        
        # テーブルの全行を取得
        all_rows = table.find_all('tr')
        data = []
        current_date = sample_date
        
        # ヘッダー行をスキップ（最初の2行）
        data_rows = all_rows[2:] if len(all_rows) > 2 else all_rows
        
        for row in data_rows:
            cols = row.find_all(['th', 'td'])
            if not cols or len(cols) < 17:  # 必要な列が揃っていない場合はスキップ
                continue
                
            # 日付行の処理
            date_match = re.search(r"(\d+)日", cols[0].get_text(strip=True))
            if date_match:
                try:
                    day = int(date_match.group(1))
                    current_date = date(sample_date.year, sample_date.month, day)
                except (ValueError, IndexError) as e:
                    logger.warning(f"日付の解析に失敗しました: {cols[0].get_text()}, {e}")
                continue
                
            # 時間データの処理
            hour = self._parse_hour(cols[0].get_text(strip=True))
            if hour is None:
                continue
                
            try:
                timestamp = self._parse_datetime(current_date, hour)
                
                # 基本情報で初期化
                row_data = {
                    'date': timestamp.date(),
                    'time': timestamp.time(),
                    'datetime': timestamp,
                    'raw_data': '|'.join(td.get_text(strip=True) for td in cols)
                }
                
                # 列のインデックスに基づいてデータを追加
                for col_idx, field_name in self.COLUMN_MAPPING.items():
                    if col_idx >= len(cols):
                        continue
                        
                    text = cols[col_idx].get_text(strip=True)
                    
                    # フィールドタイプに応じて適切にパース
                    if field_name in ['pressure_ground', 'pressure_sea', 'vapor_pressure', 'precipitation',
                                    'temperature', 'dew_point', 'humidity', 'wind_speed',
                                    'sunshine_hours', 'solar_radiation', 'cloud_cover', 'visibility']:
                        row_data[field_name] = self._parse_float(text)
                    elif field_name in ['snow_fall', 'snow_depth']:
                        row_data[field_name] = self._parse_snow(text)
                    elif field_name == 'weather':
                        row_data[field_name] = self._parse_weather_icon(cols[col_idx]) or text or None
                    elif field_name == 'wind_direction':
                        row_data[field_name] = text or None
                    else:
                        # その他のフィールドは生のテキストをそのまま保存
                        row_data[field_name] = text or None
                
                # データを追加
                data.append(row_data)
                
            except Exception as e:
                logger.warning(f"行の解析中にエラーが発生しました: {str(e)}\n行データ: {[td.get_text(strip=True) for td in cols]}")
                continue
        
        logger.info(f"S1 hourly table parsing completed successfully. Parsed {len(data)} records.")
        return data


def create_hourly_parser(obs_type: str) -> BaseHourlyTableParser:
    """観測所タイプに応じたパーサーを作成
    
    Args:
        obs_type: 観測所タイプ ('a1' または 's1')
        
    Returns:
        BaseHourlyTableParser: 時間別データパーサー
        
    Raises:
        ValueError: サポートされていない観測所タイプが指定された場合
    """
    if obs_type == 'a1':
        return HourlyTableParserA1()
    elif obs_type == 's1':
        return HourlyTableParserS1()
    else:
        raise ValueError(f"サポートされていない観測所タイプです: {obs_type}")
