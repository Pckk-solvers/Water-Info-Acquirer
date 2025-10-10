"""
テストデータとフィクスチャの整備

HTMLスニペットのテストフィクスチャ作成、モックデータとスタブの実装、
テスト用の観測所コードとサンプルデータ準備
"""

import pytest
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any
from unittest.mock import Mock
from datetime import datetime, timedelta

from src.wia.types import DataRequest, StationInfo, ExcelOptions, ChartConfig


class TestFixtureLoader:
    """テストフィクスチャローダー"""
    
    @staticmethod
    def get_fixtures_dir() -> Path:
        """フィクスチャディレクトリのパスを取得"""
        return Path(__file__).parent / "fixtures"
    
    @staticmethod
    def load_html_fixture(filename: str) -> str:
        """HTMLフィクスチャファイルを読み込み"""
        fixture_path = TestFixtureLoader.get_fixtures_dir() / filename
        if not fixture_path.exists():
            raise FileNotFoundError(f"フィクスチャファイルが見つかりません: {fixture_path}")
        
        with open(fixture_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    @staticmethod
    def get_available_fixtures() -> List[str]:
        """利用可能なフィクスチャファイルのリストを取得"""
        fixtures_dir = TestFixtureLoader.get_fixtures_dir()
        if not fixtures_dir.exists():
            return []
        
        return [f.name for f in fixtures_dir.glob("*.html")]


class MockDataGenerator:
    """モックデータ生成器"""
    
    @staticmethod
    def create_station_info(
        code: str = "12345",
        name: str = "テスト観測所",
        raw_name: str = "テスト観測所（てすとかんそくしょ）"
    ) -> StationInfo:
        """観測所情報のモックデータを作成"""
        return StationInfo(
            code=code,
            name=name,
            raw_name=raw_name
        )
    
    @staticmethod
    def create_data_request(
        code: str = "12345",
        start_year: int = 2023,
        start_month: int = 1,
        end_year: int = 2023,
        end_month: int = 12,
        mode: str = "S",
        granularity: str = "hour"
    ) -> DataRequest:
        """データリクエストのモックデータを作成"""
        return DataRequest(
            code=code,
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            mode=mode,
            granularity=granularity
        )
    
    @staticmethod
    def create_timeseries_dataframe(
        start_date: str = "2023-01-01",
        periods: int = 24,
        freq: str = "H",
        base_value: float = 1.0,
        increment: float = 0.1,
        missing_indices: List[int] = None
    ) -> pd.DataFrame:
        """時系列データのモックDataFrameを作成"""
        # 日時データ生成
        dates = pd.date_range(start_date, periods=periods, freq=freq)
        
        # 値データ生成
        values = [base_value + i * increment for i in range(periods)]
        
        # 欠損値の設定
        if missing_indices:
            for idx in missing_indices:
                if 0 <= idx < len(values):
                    values[idx] = None
        
        # display_dt計算（時間次の場合は+1時間、日次の場合はそのまま）
        if freq == "H":
            display_dates = dates + pd.Timedelta(hours=1)
        else:
            display_dates = dates
        
        # sheet_year計算
        sheet_years = [dt.year for dt in dates]
        
        return pd.DataFrame({
            'datetime': dates,
            'value': values,
            'display_dt': display_dates,
            'sheet_year': sheet_years
        })
    
    @staticmethod
    def create_excel_options(
        single_sheet: bool = False,
        include_summary: bool = True,
        chart_title: str = "テストチャート",
        y_axis_label: str = "水位[m]"
    ) -> ExcelOptions:
        """Excel出力オプションのモックデータを作成"""
        chart_config = ChartConfig(
            title=chart_title,
            y_axis_label=y_axis_label,
            x_axis_format="m"
        )
        
        return ExcelOptions(
            single_sheet=single_sheet,
            include_summary=include_summary,
            chart_config=chart_config
        )


class MockResponseGenerator:
    """HTTPレスポンスのモック生成器"""
    
    @staticmethod
    def create_mock_response(html_content: str, encoding: str = "euc_jp") -> Mock:
        """HTTPレスポンスのモックを作成"""
        mock_response = Mock()
        mock_response.text = html_content
        mock_response.encoding = encoding
        mock_response.status_code = 200
        return mock_response
    
    @staticmethod
    def create_error_response(status_code: int = 404, error_message: str = "Not Found") -> Mock:
        """エラーレスポンスのモックを作成"""
        mock_response = Mock()
        mock_response.status_code = status_code
        mock_response.text = f"<html><body><h1>{status_code} {error_message}</h1></body></html>"
        mock_response.encoding = "utf-8"
        mock_response.raise_for_status.side_effect = Exception(f"HTTP {status_code}: {error_message}")
        return mock_response


class TestDataSamples:
    """テスト用サンプルデータ"""
    
    # 観測所コードのサンプル
    SAMPLE_STATION_CODES = {
        "water_level": ["12345", "23456", "34567"],
        "flow_rate": ["45678", "56789", "67890"],
        "rainfall": ["78901", "89012", "90123"]
    }
    
    # 観測所情報のサンプル
    SAMPLE_STATION_INFO = {
        "12345": {
            "name": "神野瀬川",
            "raw_name": "神野瀬川（かんのせがわ）",
            "river": "神野瀬川"
        },
        "45678": {
            "name": "流量観測所",
            "raw_name": "流量観測所（りゅうりょうかんそくしょ）",
            "river": "テスト川"
        },
        "78901": {
            "name": "雨量観測所",
            "raw_name": "雨量観測所（うりょうかんそくしょ）",
            "river": "テスト川"
        }
    }
    
    # データ値のサンプル範囲
    SAMPLE_VALUE_RANGES = {
        "S": {"min": 0.5, "max": 5.0, "unit": "m"},      # 水位
        "R": {"min": 10.0, "max": 100.0, "unit": "m^3/s"}, # 流量
        "U": {"min": 0.0, "max": 50.0, "unit": "mm/h"}   # 雨量
    }
    
    @classmethod
    def get_sample_codes_for_mode(cls, mode: str) -> List[str]:
        """モードに対応するサンプル観測所コードを取得"""
        mode_mapping = {
            "S": "water_level",
            "R": "flow_rate", 
            "U": "rainfall"
        }
        return cls.SAMPLE_STATION_CODES.get(mode_mapping.get(mode, "water_level"), [])
    
    @classmethod
    def get_sample_station_info(cls, code: str) -> Dict[str, str]:
        """観測所コードに対応するサンプル観測所情報を取得"""
        return cls.SAMPLE_STATION_INFO.get(code, {
            "name": f"観測所{code}",
            "raw_name": f"観測所{code}（かんそくしょ{code}）",
            "river": "テスト川"
        })
    
    @classmethod
    def get_sample_value_range(cls, mode: str) -> Dict[str, Any]:
        """モードに対応するサンプル値範囲を取得"""
        return cls.SAMPLE_VALUE_RANGES.get(mode, cls.SAMPLE_VALUE_RANGES["S"])


# Pytestフィクスチャ定義
@pytest.fixture
def fixture_loader():
    """フィクスチャローダーのフィクスチャ"""
    return TestFixtureLoader()


@pytest.fixture
def mock_data_generator():
    """モックデータ生成器のフィクスチャ"""
    return MockDataGenerator()


@pytest.fixture
def mock_response_generator():
    """モックレスポンス生成器のフィクスチャ"""
    return MockResponseGenerator()


@pytest.fixture
def sample_station_info():
    """サンプル観測所情報のフィクスチャ"""
    return MockDataGenerator.create_station_info()


@pytest.fixture
def sample_data_request():
    """サンプルデータリクエストのフィクスチャ"""
    return MockDataGenerator.create_data_request()


@pytest.fixture
def sample_timeseries_data():
    """サンプル時系列データのフィクスチャ"""
    return MockDataGenerator.create_timeseries_dataframe()


@pytest.fixture
def sample_excel_options():
    """サンプルExcel出力オプションのフィクスチャ"""
    return MockDataGenerator.create_excel_options()


@pytest.fixture
def water_level_html():
    """水位データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("sample_water_data.html")


@pytest.fixture
def rain_data_html():
    """雨量データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("sample_rain_data.html")


@pytest.fixture
def flow_data_html():
    """流量データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("sample_flow_data.html")


@pytest.fixture
def empty_data_html():
    """空データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("empty_data.html")


@pytest.fixture
def malformed_data_html():
    """不正データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("malformed_data.html")


@pytest.fixture
def daily_data_html():
    """日次データHTMLフィクスチャ"""
    return TestFixtureLoader.load_html_fixture("daily_data_sample.html")


# テストフィクスチャの動作確認テスト
class TestFixtureLoading:
    """フィクスチャ読み込みのテスト"""
    
    def test_fixture_loader_initialization(self, fixture_loader):
        """フィクスチャローダーの初期化テスト"""
        assert fixture_loader is not None
        assert isinstance(fixture_loader, TestFixtureLoader)
    
    def test_fixtures_directory_exists(self, fixture_loader):
        """フィクスチャディレクトリの存在確認"""
        fixtures_dir = fixture_loader.get_fixtures_dir()
        assert fixtures_dir.exists()
        assert fixtures_dir.is_dir()
    
    def test_load_water_level_fixture(self, fixture_loader):
        """水位データフィクスチャの読み込みテスト"""
        html_content = fixture_loader.load_html_fixture("sample_water_data.html")
        assert "神野瀬川" in html_content
        assert "1.23" in html_content
    
    def test_load_empty_data_fixture(self, fixture_loader):
        """空データフィクスチャの読み込みテスト"""
        html_content = fixture_loader.load_html_fixture("empty_data.html")
        assert "テスト観測所" in html_content
        assert "データなし" in html_content
    
    def test_get_available_fixtures(self, fixture_loader):
        """利用可能フィクスチャの取得テスト"""
        fixtures = fixture_loader.get_available_fixtures()
        assert len(fixtures) > 0
        assert "sample_water_data.html" in fixtures
        assert "empty_data.html" in fixtures


class TestMockDataGeneration:
    """モックデータ生成のテスト"""
    
    def test_create_station_info(self, mock_data_generator):
        """観測所情報モックデータ作成テスト"""
        station_info = mock_data_generator.create_station_info()
        assert station_info.code == "12345"
        assert station_info.name == "テスト観測所"
        assert "てすとかんそくしょ" in station_info.raw_name
    
    def test_create_data_request(self, mock_data_generator):
        """データリクエストモックデータ作成テスト"""
        request = mock_data_generator.create_data_request()
        assert request.code == "12345"
        assert request.start_year == 2023
        assert request.mode == "S"
        assert request.granularity == "hour"
    
    def test_create_timeseries_dataframe(self, mock_data_generator):
        """時系列DataFrameモックデータ作成テスト"""
        df = mock_data_generator.create_timeseries_dataframe()
        
        # 基本構造の確認
        expected_columns = ['datetime', 'value', 'display_dt', 'sheet_year']
        assert list(df.columns) == expected_columns
        assert len(df) == 24  # デフォルトは24時間
        
        # データ型の確認
        assert pd.api.types.is_datetime64_any_dtype(df['datetime'])
        assert pd.api.types.is_numeric_dtype(df['value'])
        assert pd.api.types.is_datetime64_any_dtype(df['display_dt'])
        assert pd.api.types.is_integer_dtype(df['sheet_year'])
    
    def test_create_timeseries_dataframe_with_missing_values(self, mock_data_generator):
        """欠損値ありの時系列DataFrameモックデータ作成テスト"""
        df = mock_data_generator.create_timeseries_dataframe(
            periods=10,
            missing_indices=[2, 5, 8]
        )
        
        # 欠損値の確認
        assert pd.isna(df.iloc[2]['value'])
        assert pd.isna(df.iloc[5]['value'])
        assert pd.isna(df.iloc[8]['value'])
        
        # 非欠損値の確認
        assert not pd.isna(df.iloc[0]['value'])
        assert not pd.isna(df.iloc[1]['value'])
    
    def test_create_excel_options(self, mock_data_generator):
        """Excel出力オプションモックデータ作成テスト"""
        options = mock_data_generator.create_excel_options()
        
        assert options.single_sheet is False
        assert options.include_summary is True
        assert options.chart_config is not None
        assert options.chart_config.title == "テストチャート"


class TestSampleData:
    """サンプルデータのテスト"""
    
    def test_sample_station_codes(self):
        """サンプル観測所コードのテスト"""
        water_codes = TestDataSamples.get_sample_codes_for_mode("S")
        flow_codes = TestDataSamples.get_sample_codes_for_mode("R")
        rain_codes = TestDataSamples.get_sample_codes_for_mode("U")
        
        assert len(water_codes) > 0
        assert len(flow_codes) > 0
        assert len(rain_codes) > 0
        
        # 重複がないことを確認
        all_codes = water_codes + flow_codes + rain_codes
        assert len(all_codes) == len(set(all_codes))
    
    def test_sample_station_info(self):
        """サンプル観測所情報のテスト"""
        info = TestDataSamples.get_sample_station_info("12345")
        assert "name" in info
        assert "raw_name" in info
        assert "river" in info
        
        # 存在しないコードの場合のデフォルト値確認
        default_info = TestDataSamples.get_sample_station_info("99999")
        assert "観測所99999" in default_info["name"]
    
    def test_sample_value_ranges(self):
        """サンプル値範囲のテスト"""
        for mode in ["S", "R", "U"]:
            range_info = TestDataSamples.get_sample_value_range(mode)
            assert "min" in range_info
            assert "max" in range_info
            assert "unit" in range_info
            assert range_info["min"] < range_info["max"]


if __name__ == "__main__":
    pytest.main([__file__])