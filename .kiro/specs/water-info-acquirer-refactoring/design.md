# 設計書

## 概要

Water-Info-Acquirerプロジェクトのリファクタリング設計書です。現在の単一ファイルに集中した実装を、責務分離・共通化・テスト容易性を考慮したモジュラー構造に再編します。外部仕様（CLI・Excel出力形式）は維持しながら、内部構造を段階的に改善します。

## アーキテクチャ

### 現在の構造の問題点

- `src/main_datetime.py`：GUI・データ取得・Excel出力・エラーハンドリングが混在（約600行）
- `src/datemode.py`：類似のデータ取得・Excel出力処理が重複実装
- 共通ロジックの重複：URL生成・観測所名抽出・Excel出力・チャート作成
- テストが困難：UIとビジネスロジックが密結合
- エラーハンドリングが不統一：例外処理とログが散在

### 目標アーキテクチャ

```
src/
├── wia/                          # 新パッケージ
│   ├── __init__.py              # パッケージ初期化
│   ├── data_source.py           # データ取得・整形の統合レイヤ
│   ├── excel_writer.py          # Excel出力の共通レイヤ
│   ├── gui.py                   # GUI分離（薄いUI層）
│   ├── errors.py                # 例外クラス定義
│   ├── constants.py             # モード設定・定数管理
│   └── types.py                 # 型定義・Protocol
├── __main__.py                  # CLIエントリーポイント（既存維持）
└── main_datetime.py             # 段階的移行用（最終的に削除）
```

### レイヤー構造

```
┌─────────────────┐
│   Presentation  │  GUI (gui.py) / CLI (__main__.py)
├─────────────────┤
│   Application   │  データ取得・Excel出力の統合API
├─────────────────┤
│   Domain        │  データモデル・ビジネスルール
├─────────────────┤
│   Infrastructure│  HTTP通信・ファイル出力
└─────────────────┘
```

## コンポーネントと インターフェース

### 1. データ取得レイヤ（data_source.py）

#### 責務
- 時間次・日次データ取得の統合
- URL生成・観測所名抽出の共通化
- HTML解析・数値変換の標準化

#### 主要インターフェース

```python
@dataclass
class DataRequest:
    code: str
    start_year: int
    start_month: int
    end_year: int
    end_month: int
    mode: Literal["S", "R", "U"]  # 水位・流量・雨量
    granularity: Literal["hour", "day"]

@dataclass
class StationInfo:
    code: str
    name: str
    raw_name: str

def fetch_station_info(code: str, mode: str) -> StationInfo:
    """観測所情報を取得"""

def fetch_timeseries_data(request: DataRequest) -> pd.DataFrame:
    """時系列データを取得（datetime, value列を持つDataFrame）"""

def generate_url(code: str, mode: str, date_range: str, granularity: str) -> str:
    """URL生成の共通ロジック"""
```

#### 内部設計

```python
# constants.py で管理される設定
MODE_CONFIG = {
    "S": {
        "hour": {"num": "2", "base_url": "DspWaterData.exe", "unit": "m"},
        "day": {"num": "3", "base_url": "DspWaterData.exe", "unit": "m"}
    },
    "R": {
        "hour": {"num": "6", "base_url": "DspWaterData.exe", "unit": "m^3/s"},
        "day": {"num": "7", "base_url": "DspWaterData.exe", "unit": "m^3/s"}
    },
    "U": {
        "hour": {"num": "2", "base_url": "DspRainData.exe", "unit": "mm/h"},
        "day": {"num": "3", "base_url": "DspRainData.exe", "unit": "mm/h"}
    }
}
```

### 2. Excel出力レイヤ（excel_writer.py）

#### 責務
- 全期間・年別・summaryシートの統一出力
- 散布図挿入の共通化
- ファイル名生成の標準化

#### 主要インターフェース

```python
@dataclass
class ExcelOptions:
    single_sheet: bool = False
    include_summary: bool = True
    chart_config: Optional[ChartConfig] = None

@dataclass
class ChartConfig:
    title: str
    y_axis_label: str
    x_axis_format: str
    size: Tuple[int, int] = (720, 300)

def write_timeseries_excel(
    df: pd.DataFrame,
    station_info: StationInfo,
    request: DataRequest,
    options: ExcelOptions
) -> Path:
    """統合Excel出力API"""

def create_yearly_sheets(df: pd.DataFrame, writer: pd.ExcelWriter, config: dict):
    """年別シート作成"""

def create_summary_sheet(df: pd.DataFrame, writer: pd.ExcelWriter, config: dict):
    """サマリーシート作成"""

def insert_chart(worksheet, df: pd.DataFrame, config: ChartConfig, position: str):
    """散布図挿入の共通ロジック"""
```

### 3. GUI分離レイヤ（gui.py）

#### 責務
- UI コンポーネントの管理（入力・表示のみ）
- ビジネスロジックへの委譲
- エラー表示の統一

#### 主要インターフェース

```python
class WWRApp:
    def __init__(self, single_sheet_mode: bool = False):
        """GUI初期化"""
    
    def _on_execute(self):
        """実行ボタンハンドラ（ビジネスロジック呼び出し）"""
    
    def _show_error(self, message: str):
        """エラー表示の統一"""
    
    def _show_results(self, files: List[Path]):
        """結果表示"""

# ビジネスロジック呼び出し例
def execute_data_acquisition(codes: List[str], request: DataRequest, options: ExcelOptions) -> List[Path]:
    """データ取得・Excel出力の統合処理"""
```

### 4. エラーハンドリング（errors.py）

#### 責務
- 業務例外の定義
- エラーメッセージの標準化

```python
class WaterInfoAcquirerError(Exception):
    """基底例外クラス"""

class EmptyDataError(WaterInfoAcquirerError):
    """データが空の場合の例外"""
    
class NetworkError(WaterInfoAcquirerError):
    """ネットワークエラー"""
    
class ParseError(WaterInfoAcquirerError):
    """HTML解析エラー"""
```

### 5. 定数・設定管理（constants.py）

```python
# モード設定
MODE_LABELS = {"S": "水位", "R": "流量", "U": "雨量"}
MODE_UNITS = {"S": "m", "R": "m^3/s", "U": "mm/h"}
MODE_FILE_SUFFIXES = {"S": "WH", "R": "QH", "U": "RH"}

# URL設定
BASE_URL = "http://www1.river.go.jp/cgi-bin/"
ENCODING = "euc_jp"

# Excel設定
DEFAULT_CHART_SIZE = (720, 300)
COLUMN_WIDTHS = {"datetime": 20, "value": 12, "summary": 15}
```

## データモデル

### 時系列データの標準化

```python
# 統一されたDataFrame構造
columns = ["datetime", "value", "display_dt", "sheet_year"]

# datetime: 実測定時刻（0:00-23:00）
# display_dt: 表示用時刻（1:00-0:00、+1時間シフト）
# value: 測定値（数値またはNaN）
# sheet_year: シート分割用年（実測定時刻ベース）
```

### 設定データの構造化

```python
@dataclass
class ModeConfig:
    label: str
    unit: str
    file_suffix: str
    hour_config: UrlConfig
    day_config: UrlConfig

@dataclass
class UrlConfig:
    num: str
    base_url: str
    encoding: str = "euc_jp"
```

## エラーハンドリング

### エラー分類と対応方針

1. **業務例外**（EmptyDataError等）
   - GUI：ポップアップ表示、処理継続
   - CLI：非ゼロ終了コード、エラーメッセージ出力

2. **システム例外**（NetworkError等）
   - ログ出力、短文通知
   - 処理中断、適切な終了コード

3. **想定外例外**
   - 詳細ログ出力、スタックトレース保存
   - ユーザーには簡潔なエラーメッセージ

### ログ設定

```python
# logging設定例
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('water_info_acquirer.log'),
        logging.StreamHandler()
    ]
)
```

## テスト戦略

### テスト分類

1. **ユニットテスト**
   - URL生成ロジック
   - 期間分割処理
   - 数値変換処理
   - ファイル名生成

2. **統合テスト**
   - データ取得フロー
   - Excel出力フロー
   - エラーハンドリング

3. **スモークテスト**
   - Excel出力の存在確認
   - シート・列・チャートの基本構造確認
   - CLI・GUIの基本動作確認

### テストデータ戦略

```python
# HTMLスニペットをテストリソースとして保存
tests/
├── fixtures/
│   ├── sample_water_data.html
│   ├── sample_rain_data.html
│   └── empty_data.html
├── test_data_source.py
├── test_excel_writer.py
└── test_integration.py
```

### モックとスタブ

```python
# requests.get のモック例
@patch('wia.data_source.requests.get')
def test_fetch_data_success(mock_get):
    mock_response = Mock()
    mock_response.text = load_fixture('sample_water_data.html')
    mock_response.encoding = 'euc_jp'
    mock_get.return_value = mock_response
    
    result = fetch_timeseries_data(test_request)
    assert len(result) > 0
```

## パフォーマンス最適化

### HTTP通信の最適化

```python
# requests.Session の活用
class DataFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Water-Info-Acquirer/1.0'})
    
    def fetch_data(self, url: str) -> str:
        response = self.session.get(url)
        response.encoding = 'euc_jp'
        return response.text
```

### 並列処理の導入

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_multiple_stations(requests: List[DataRequest]) -> List[pd.DataFrame]:
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_timeseries_data, req): req for req in requests}
        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to fetch data: {e}")
        return results
```

## 移行戦略

### 段階的リファクタリング

1. **P1: データ取得コア統合**
   - `wia/data_source.py` 作成
   - 既存関数の段階的置換
   - 出力データの一致確認

2. **P2: Excel出力共通化**
   - `wia/excel_writer.py` 作成
   - 既存Excel出力の置換
   - 出力形式の一致確認

3. **P3: GUI分離**
   - `wia/gui.py` 作成
   - UIとビジネスロジックの分離
   - 動作確認

4. **P4: エラー・ログ整備**
   - 統一ログ設定
   - 例外ハンドリング標準化

5. **P5: パッケージ構造移行**
   - 最終ディレクトリ構造への移行
   - import文の整理

### 互換性保証

- CLI オプション（`--single-sheet`）の維持
- Excel出力形式（シート名・列名・チャート）の維持
- PyInstaller ビルドプロセスの維持
- エラーメッセージの一貫性

### リスク軽減策

- 小さなPR単位での変更
- 各段階での回帰テスト実施
- 既存機能の動作確認
- ロールバック可能な設計

## 品質メトリクス

### コード品質

- 重複コード削減：取得・Excel出力の実装箇所を1系統に集約
- テストカバレッジ：主要パスの80%以上
- 静的解析：Ruff・Black・mypyによる品質チェック

### 運用品質

- ログ出力：INFO/ERRORレベルでの適切な情報記録
- エラーハンドリング：業務例外と技術例外の明確な分離
- パフォーマンス：Session化による通信効率改善

この設計により、保守性・拡張性・テスト容易性を大幅に向上させながら、既存ユーザーへの影響を最小限に抑えたリファクタリングを実現します。