import re
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Any, Optional, Tuple
from bs4 import Tag, BeautifulSoup
from .table_parser import TableParser
from jma_rainfall_pipeline.logger.app_logger import get_logger
from .date_utils import extract_date_from_html

logger = get_logger(__name__)

class Minute10TableParser(TableParser):
    """10分間隔データ用テーブルパーサー（A1/S1形式対応）"""
    S1_FIELD_MAP = [
        ("pressure_ground", 1, "float"),
        ("pressure_sea", 2, "float"),
        ("precipitation", 3, "float"),
        ("temperature", 4, "float"),
        ("humidity", 5, "float"),
        ("wind_speed", 6, "float"),
        ("wind_direction", 7, "text"),
        ("wind_speed_max", 8, "float"),
        ("wind_direction_max", 9, "text"),
        ("sunshine_minutes", 10, "float"),
    ]

    A1_FIELD_MAP = [
        ("precipitation", 1, "float"),
        ("temperature", 2, "float"),
        ("humidity", 3, "float"),
        ("wind_speed", 4, "float"),
        ("wind_direction", 5, "text"),
        ("wind_speed_max", 6, "float"),
        ("wind_direction_max", 7, "text"),
        ("sunshine_minutes", 8, "float"),
    ]

    
    def can_parse(self, table: Tag) -> bool:
        """10分間隔データのテーブルかどうかを判定
        
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
        
        # 必須カラム（時刻、降水量、気温など）
        # A1形式: ['時分', '降水量', '気温', ...]
        # S1形式: ['時分', '気圧(hPa)', '降水量(mm)', '気温(℃)', ...]
        header_text = ' '.join(first_row_headers)
        
        # ヘッダーのバリエーションを考慮してチェック
        has_time = any(x in header_text for x in ['時分', '時刻', '時間'])
        has_precipitation = any(x in header_text for x in ['降水量(mm)', '降水量'])
        has_temperature = any(x in header_text for x in ['気温(℃)', '気温'])
        
        if not (has_time and has_precipitation):
            return False

        # 3. データ行の確認（複数行を確認）
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
            
            # 時刻セルをチェック（1列目）
            time_cell = row[0].get_text(strip=True)
            
            # 時刻形式のチェック（HH:MM形式）
            if re.search(r'^\d{1,2}:\d{2}$', time_cell):
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
                
            # 時刻セルのチェック（1列目）
            time_cell = first_data_row[0].get_text(strip=True)
            
            # 時刻形式の最終チェック（HH:MM形式）
            if not re.search(r'^\d{1,2}:\d{2}$', time_cell):
                return False
                
        except (IndexError, AttributeError) as e:
            logger.debug(f"テーブル検証エラー: {e}")
            return False

        return True
        
    def find_table(self, soup) -> Optional[Tag]:
        """10分間隔データのテーブルを探して返す"""
        # 気象庁の10分間隔データテーブルを特定するためのセレクタ
        selectors = [
            'table#tablefix1.data2_s',  # IDとクラスで特定
            'table.data2_s',  # クラス名で特定
            'table.data',  # よく使われるクラス名
            'table[summary*="10分"]',  # テーブルの説明に「10分」が含まれる場合
            'table:has(th:contains("時分"))',  # ヘッダーで特定
            'table:has(tr:has(th:contains("分")))'  # より緩やかな条件
        ]
        
        for selector in selectors:
            try:
                table = soup.select_one(selector)
                if table and self.can_parse(table):
                    return table
            except Exception as e:
                logger.debug(f"テーブルセレクター '{selector}' でエラー: {e}")
                continue
                
        # セレクタで見つからない場合は親クラスの実装にフォールバック
        return super().find_table(soup)
    
    def _determine_format(self, header_row: Tag) -> str:
        """テーブルのフォーマットを判定（A1 or S1）"""
        header_text = ' '.join([th.get_text(strip=True) for th in header_row.find_all('th')])
        
        # S1形式の判定条件を緩和
        if '気圧' in header_text:
            return 's1'
        return 'a1'
    
    def _parse_s1_headers(self, header_rows: List[Tag]) -> List[Dict[str, Any]]:
        """S1形式のヘッダー行を解析してカラム情報を返す
        
        Args:
            header_rows: ヘッダー行のリスト（2行分）
            
        Returns:
            カラム情報のリスト
        """
        if len(header_rows) < 2:
            return []
            
        # 各行のセルを取得
        rows = []
        for row in header_rows:
            rows.append(row.find_all(['th', 'td']))
            
        # カラム情報を初期化
        columns = []
        
        # 1行目のヘッダーを処理
        for i, cell in enumerate(rows[0]):
            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))
            text = cell.get_text(strip=True)
            
            # カラム情報を作成
            for _ in range(colspan):
                col_info = {
                    'header1': text if rowspan > 1 else '',
                    'header2': '',
                    'unit': ''
                }
                columns.append(col_info)
        
        # 2行目のヘッダーを処理
        col_idx = 0
        for cell in rows[1]:
            text = cell.get_text(strip=True)
            
            # 空でないセルを探す
            while col_idx < len(columns) and columns[col_idx]['header1'] != '':
                col_idx += 1
                
            if col_idx < len(columns):
                # 単位を抽出（括弧内のテキスト）
                unit_match = re.search(r'\((.*?)\)', text)
                if unit_match:
                    columns[col_idx]['unit'] = unit_match.group(1)
                columns[col_idx]['header2'] = text
                col_idx += 1
        
        # カラムキーを生成
        for i, col in enumerate(columns):
            # ヘッダーから有効な名前を選択
            name = col['header2'] or col['header1'] or f'col_{i}'
            # キーを生成（英数字とアンダースコアのみ許可）
            key = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
            col['key'] = key
            
        return columns
    
    def _assign_metrics(self, row_data: Dict[str, Any], cols: List[str], mapping: List[Tuple[str, int, str]]) -> None:
        for field, index, kind in mapping:
            value = cols[index] if index < len(cols) else ""
            if kind == "float":
                row_data[field] = self._parse_float(value)
            else:
                clean = value.strip()
                row_data[field] = clean if clean not in ("", "--", "///") else None

    def _parse_a1_headers(self, header_rows: List[Tag]) -> List[Dict[str, Any]]:
        """A1形式のヘッダー行を解析してカラム情報を返す
        
        Args:
            header_rows: ヘッダー行のリスト（3行分）
            
        Returns:
            カラム情報のリスト
        """
        if len(header_rows) < 3:
            return []
            
        # 各行のセルを取得
        rows = []
        for row in header_rows:
            rows.append(row.find_all(['th', 'td']))
            
        # カラム情報を初期化
        columns = []
        
        # 1行目のヘッダーを処理
        for i, cell in enumerate(rows[0]):
            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))
            text = cell.get_text(strip=True)
            
            # カラム情報を作成
            for _ in range(colspan):
                col_info = {
                    'header1': text if rowspan > 2 else '',
                    'header2': '',
                    'header3': '',
                    'unit': ''
                }
                columns.append(col_info)
        
        # 2行目のヘッダーを処理
        col_idx = 0
        for cell in rows[1]:
            rowspan = int(cell.get('rowspan', 1))
            colspan = int(cell.get('colspan', 1))
            text = cell.get_text(strip=True)
            
            # 空でないセルを探す
            while col_idx < len(columns) and columns[col_idx]['header1'] == '':
                col_idx += 1
                
            # セルの範囲を更新
            for _ in range(colspan):
                if col_idx < len(columns):
                    columns[col_idx]['header2'] = text
                    col_idx += 1
        
        # 3行目のヘッダーを処理
        col_idx = 0
        for cell in rows[2]:
            text = cell.get_text(strip=True)
            
            # 空でないセルを探す
            while col_idx < len(columns) and (columns[col_idx]['header1'] != '' or columns[col_idx]['header2'] != ''):
                col_idx += 1
                
            if col_idx < len(columns):
                # 単位を抽出（括弧内のテキスト）
                unit_match = re.search(r'\((.*?)\)', text)
                if unit_match:
                    columns[col_idx]['unit'] = unit_match.group(1)
                columns[col_idx]['header3'] = text
                col_idx += 1
        
        # カラムキーを生成
        for i, col in enumerate(columns):
            # ヘッダーから有効な名前を選択
            name = col['header3'] or col['header2'] or col['header1'] or f'col_{i}'
            # キーを生成（英数字とアンダースコアのみ許可）
            key = re.sub(r'[^a-zA-Z0-9_]', '_', name).lower()
            col['key'] = key
            
        return columns
        
    def _parse_a1_format(self, rows: List[Tag], sample_date: date, headers: List[str]) -> List[Dict[str, Any]]:
        """Parse rows for the A1 table variant."""
        data: List[Dict[str, Any]] = []
        current_date = sample_date

        if len(rows) < 4:
            return data

        header_meta = self._parse_a1_headers(rows[:3])

        for row in rows[3:]:
            cols = [td.get_text(strip=True) for td in row.find_all('td')]
            if not cols or not cols[0]:
                continue

            date_match = re.search(r'(\d+)日', cols[0])
            if date_match:
                try:
                    day = int(date_match.group(1))
                    current_date = date(sample_date.year, sample_date.month, day)
                except (ValueError, IndexError):
                    logger.debug('日付のパースに失敗しました')
                continue

            time_match = re.search(r'(\d+):(\d+)', cols[0])
            if not time_match:
                continue

            hour = int(time_match.group(1))
            minute = int(time_match.group(2))

            if hour == 24 and minute == 0:
                timestamp = datetime.combine(current_date, time(23, 59, 59, 999999))
            else:
                timestamp = datetime.combine(current_date, time(hour, minute))

            row_data: Dict[str, Any] = {
                'date': timestamp.date(),
                'time': timestamp.time(),
                'hour': hour,
                'minute': minute,
                'datetime': timestamp,
                '_original_headers': header_meta,
                '_format': 'a1',
                'raw_data': '|'.join(cols),
            }

            self._assign_metrics(row_data, cols, self.A1_FIELD_MAP)
            data.append(row_data)

        return data
    def _parse_s1_format(self, rows: List[Tag], sample_date: date, headers: List[str]) -> List[Dict[str, Any]]:
        """Parse rows for the S1 table variant."""
        data: List[Dict[str, Any]] = []
        current_date = sample_date

        if len(rows) < 3:
            return data

        header_meta = self._parse_s1_headers(rows[:2])

        for row in rows[2:]:
            cols = [td.get_text(strip=True) for td in row.find_all('td')]
            if not cols or not cols[0]:
                continue

            date_match = re.search(r'(\d+)日', cols[0])
            if date_match:
                try:
                    day = int(date_match.group(1))
                    current_date = date(sample_date.year, sample_date.month, day)
                except (ValueError, IndexError):
                    logger.debug('日付のパースに失敗しました')
                continue

            time_match = re.search(r'(\d+):(\d+)', cols[0])
            if not time_match:
                continue

            hour = int(time_match.group(1))
            minute = int(time_match.group(2))

            if hour == 24 and minute == 0:
                timestamp = datetime.combine(current_date, time(23, 59, 59, 999999))
            else:
                timestamp = datetime.combine(current_date, time(hour, minute))

            row_data: Dict[str, Any] = {
                'date': timestamp.date(),
                'time': timestamp.time(),
                'hour': hour,
                'minute': minute,
                'datetime': timestamp,
                '_original_headers': header_meta,
                '_format': 's1',
                'raw_data': '|'.join(cols),
            }

            self._assign_metrics(row_data, cols, self.S1_FIELD_MAP)
            data.append(row_data)

        return data
    def _get_sample_date(self, sample_date: Optional[date], html_content: Optional[str]) -> date:
        """sample_date を取得する。指定されていない場合は html_content から抽出する"""
        if sample_date is not None:
            return sample_date
            
        if html_content is None:
            raise ValueError("sample_date または html_content のいずれかは必須です")
            
        sample_date = extract_date_from_html(html_content)
        return sample_date
        
    def parse_table(self, table: Tag, sample_date: Optional[date] = None, html_content: Optional[str] = None) -> List[Dict[str, Any]]:
        """10分間隔データのテーブルをパース
        
        Args:
            table: パース対象のテーブル要素
            sample_date: サンプル日付（オプション）
            html_content: HTMLコンテンツ（オプション）
            
        Returns:
            パースされたデータのリスト（各要素は辞書）
        """
        logger.info("Starting 10-minute table parsing")
        all_rows = table.find_all('tr')
        
        # ヘッダー行を検出
        header_row = next((row for row in all_rows 
                         if row.find('th', string=lambda x: x and '時分' in str(x))), None)
        
        if not header_row:
            raise ValueError("10分間隔データのヘッダー行が見つかりません")
        
        # フォーマットを判定
        table_format = self._determine_format(header_row)
        
        # データ行を取得（ヘッダー行を除く）
        data_start = all_rows.index(header_row)
        data_rows = all_rows[data_start:]
        
        # フォーマットに応じたパースを実行
        if table_format == 's1':
            # S1形式の場合は既存のロジックを使用
            headers = [th.get_text(strip=True) for th in header_row.find_all('th')]
            return self._parse_s1_format(data_rows, sample_date, headers)
        else:  # A1 format
            # A1形式の場合は新しいパーサーを使用
            return self._parse_a1_format(data_rows, sample_date, [])
    
    def _parse_float(self, value: str) -> Optional[float]:
        """文字列をfloatに変換（エラー時はNoneを返す）"""
        if not value or value.strip() in ('--', ''):
            return None
        try:
            # カンマやその他の不要な文字を削除
            cleaned = value.replace(',', '').strip()
            return float(cleaned) if cleaned else None
        except (ValueError, TypeError):
            return None
