"""
API層の基本構造とインターフェース定義

このモジュールは、JMA降雨量データ取得APIのエントリーポイントとして機能します。
責務: リクエストの受け付け、レスポンスの生成、認証・認可

設計コンセプト:
- API層はHTTPリクエスト/レスポンスの処理のみに専念する
- ビジネスロジックはビジネスロジック層に委譲する
- インフラ処理（DBアクセス、ファイル操作）はインフラ層に委譲する

クラス構成:
1. データクラス (APIRequest, APIResponse): リクエストとレスポンスの構造を定義
2. 基底クラス (BaseAPIHandler): 共通のインターフェースを定義
3. 具象クラス (WeatherDataAPIHandler, HealthCheckAPIHandler): 具体的な処理を実装
4. ファクトリークラス (APIFactory): インスタンス生成を担当
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass

from jma_rainfall_pipeline.api.constants import CSV_ALLOWED_FIELDS, CSV_DEFAULT_FIELDS
from jma_rainfall_pipeline.api.responses.response_builder import (
    ResponseFactory,
    HTTPStatusCodes,
)
from jma_rainfall_pipeline.api.validators.request_validator import (
    WeatherDataRequestValidator,
    ValidationResult,
)


# 循環依存を避けるため、ドメインサービスは遅延インポートで扱う
try:  # pragma: no cover - サービスが存在しない場合も考慮
    from jma_rainfall_pipeline.domain.services.weather_services import WeatherDataService
except ImportError:  # pragma: no cover
    WeatherDataService = None  # type: ignore


@dataclass
class APIRequest:
    """
    APIリクエストを表現するデータクラス

    このクラスはHTTPリクエストの情報を構造化して保持します。
    FastAPIやFlaskなどのWebフレームワークから受け取ったリクエストを
    このクラスに変換して処理します。

    Attributes:
        endpoint (str): APIエンドポイントのパス（例: "/weather/data"）
        method (str): HTTPメソッド（GET, POST, PUT, DELETE）
        parameters (Dict[str, Any]): リクエストパラメータ（クエリパラメータ、POSTデータ）
        headers (Optional[Dict[str, str]]): HTTPヘッダー情報

    Example:
        >>> request = APIRequest(
        ...     endpoint="/weather/data",
        ...     method="POST",
        ...     parameters={
        ...         "stations": [["11", "47401", "s1"]],
        ...         "start_date": "2024-01-01",
        ...         "end_date": "2024-01-31"
        ...     }
        ... )
    """
    endpoint: str
    method: str
    parameters: Dict[str, Any]
    headers: Optional[Dict[str, str]] = None


@dataclass
class APIResponse:
    """
    APIレスポンスを表現するデータクラス

    このクラスはAPIからのレスポンス情報を構造化して保持します。
    Webフレームワークに返すレスポンスオブジェクトに変換されます。

    Attributes:
        status_code (int): HTTPステータスコード（200, 400, 500など）
        data (Any): レスポンス本体データ（JSON、CSVデータなど）
        message (Optional[str]): ユーザーに表示するメッセージ
        timestamp (Optional[datetime]): レスポンス生成時刻

    Example:
        >>> response = APIResponse(
        ...     status_code=200,
        ...     data={"temperature": 20.5, "humidity": 65},
        ...     message="Data retrieved successfully"
        ... )
    """
    status_code: int
    data: Any
    message: Optional[str] = None
    timestamp: Optional[datetime] = None
    content_type: str = "application/json"
    headers: Optional[Dict[str, str]] = None


class RequestValidationError(ValueError):
    """入力検証で不正なパラメータが見つかった際に送出される例外。"""

    def __init__(self, errors: list[str]):
        super().__init__("入力検証に失敗しました")
        self.errors = errors


class BaseAPIHandler(ABC):
    """
    APIハンドラーの基底クラス

    すべてのAPIハンドラーが実装すべき共通インターフェースを定義します。
    テンプレートメソッドパターンを採用し、処理の流れを共通化します。

    サブクラスは以下のメソッドを実装する必要があります：
    - handle_request: メインの処理ロジック
    - validate_request: リクエストの検証ロジック

    設計上の利点:
    - コードの重複を防ぐ
    - 処理の流れを標準化する
    - テストがしやすくなる
    """

    @abstractmethod
    def handle_request(self, request: APIRequest) -> APIResponse:
        """
        リクエストを処理する

        Args:
            request (APIRequest): 処理対象のリクエスト

        Returns:
            APIResponse: 処理結果のレスポンス
        """
        pass

    @abstractmethod
    def validate_request(self, request: APIRequest) -> bool:
        """
        リクエストをバリデーションする

        Args:
            request (APIRequest): 検証対象のリクエスト

        Returns:
            bool: 検証結果（True: 有効、False: 無効）
        """
        pass


class WeatherDataAPIHandler(BaseAPIHandler):
    """
    気象データ取得APIハンドラー

    JMA気象データ取得の具体的な処理を実装します。
    このクラスは以下の流れで処理を行います：

    1. リクエストの検証（validate_request）
    2. ビジネスロジック層への処理委譲
    3. レスポンスの生成

    依存関係:
    - ビジネスロジック層（WeatherDataService）と連携
    - インフラ層（データアクセス、キャッシュ）と連携

    Attributes:
        _business_service: ビジネスロジックサービス（依存性注入で設定）
    """

    def __init__(self):
        """初期化処理"""
        self._business_service: Optional[WeatherDataService] = None
        self._validator: Optional[WeatherDataRequestValidator] = None
        self._response_builder = ResponseFactory.create_json_builder()

    # ------------------------------------------------------------------
    # 依存性注入（DI）用のセッター
    # ------------------------------------------------------------------
    def set_business_service(self, service: WeatherDataService) -> None:
        """ビジネスロジックサービスを設定"""
        self._business_service = service

    def set_validator(self, validator: WeatherDataRequestValidator) -> None:
        """入力バリデータを設定"""
        self._validator = validator

    def set_response_builder(self, pretty_print: bool = False) -> None:
        """レスポンスビルダーを設定（JSON出力を制御）"""
        self._response_builder = ResponseFactory.create_json_builder(pretty_print=pretty_print)

    def handle_request(self, request: APIRequest) -> APIResponse:
        """
        気象データ取得リクエストを処理

        処理の流れ:
        1. リクエストの検証
        2. ビジネスロジック層の呼び出し
        3. エラーハンドリング

        Args:
            request (APIRequest): 気象データ取得リクエスト

        Returns:
            APIResponse: 処理結果（データまたはエラー情報）
        """
        # ステップ1: リクエストの検証
        if not self.validate_request(request):
            return self._build_error_response(
                status_code=HTTPStatusCodes.BAD_REQUEST,
                message="Invalid request parameters",
                details={"missing_fields": self._missing_required_fields(request)}
            )

        try:
            # ステップ2: ビジネスロジック層を呼び出す
            sanitized_payload = self._run_validator(request)

            if not self._business_service:
                raise RuntimeError("Business service is not configured. Call set_business_service() before use.")

            result = self._business_service.process_weather_data_request(sanitized_payload)

            if result.errors:
                return self._build_error_response(
                    status_code=HTTPStatusCodes.UNPROCESSABLE_ENTITY,
                    message="Business rule validation failed",
                    details={"errors": result.errors}
                )

            output_format = str(sanitized_payload.get("output_format", "json")).lower()
            if output_format == "csv":
                return self._build_csv_response(result, sanitized_payload.get("fields"))

            response_payload = result.to_dict(include_records=True)

            api_response = APIResponse(
                status_code=HTTPStatusCodes.OK,
                data=response_payload,
                message="気象データの取得に成功しました",
                timestamp=datetime.now()
            )
            return api_response

        except RequestValidationError as exc:
            return self._build_error_response(
                status_code=HTTPStatusCodes.BAD_REQUEST,
                message="Invalid request parameters",
                details={"errors": exc.errors}
            )
        except Exception as e:
            # ステップ3: エラーハンドリング
            return self._build_error_response(
                status_code=HTTPStatusCodes.INTERNAL_SERVER_ERROR,
                message="Internal server error",
                details={"reason": str(e)}
            )

    def validate_request(self, request: APIRequest) -> bool:
        """
        気象データリクエストのパラメータをバリデーション

        必須パラメータの存在チェックのみをここで行い、
        詳細なバリデーションはバリデータークラスに委譲します。

        Args:
            request (APIRequest): 検証対象のリクエスト

        Returns:
            bool: パラメータが有効かどうか
        """
        return len(self._missing_required_fields(request)) == 0

    # ------------------------------------------------------------------
    # 内部ユーティリティ
    # ------------------------------------------------------------------
    def _missing_required_fields(self, request: APIRequest) -> list[str]:
        required_params = ['stations', 'start_date', 'end_date']
        return [param for param in required_params if param not in request.parameters]

    def _run_validator(self, request: APIRequest) -> Dict[str, Any]:
        """設定済みのバリデータを実行し、サニタイズ済みデータを返す"""
        if not self._validator:
            return request.parameters

        validation_result: ValidationResult = self._validator.validate(request)
        if not validation_result.is_valid:
            raise RequestValidationError(validation_result.errors)

        return validation_result.sanitized_data or request.parameters

    def _build_error_response(
        self,
        *,
        status_code: int,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> APIResponse:
        payload: Dict[str, Any] = {
            "message": message,
            "details": details or {},
        }
        return APIResponse(
            status_code=status_code,
            data=payload,
            message=message,
            timestamp=datetime.now(),
            content_type="application/json"
        )

    def _build_csv_response(
        self,
        result: "WeatherDataResult",
        fields: Optional[List[str]]
    ) -> APIResponse:
        raw_fields = list(fields) if fields else list(CSV_DEFAULT_FIELDS)
        selected_fields = [field for field in raw_fields if field in CSV_ALLOWED_FIELDS]
        if not selected_fields:
            selected_fields = list(CSV_DEFAULT_FIELDS)
        csv_content = result.to_csv(selected_fields)
        filename = f"weather_data_{datetime.now().strftime('%Y%m%d%H%M%S')}" + ".csv"
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        return APIResponse(
            status_code=HTTPStatusCodes.OK,
            data=csv_content,
            message="CSV export generated successfully",
            timestamp=datetime.now(),
            content_type="text/csv",
            headers=headers
        )

class APIFactory:
    """
    APIハンドラーのファクトリークラス

    APIハンドラーのインスタンス生成を担当します。
    依存性注入（DI）のパターンを採用し、テストしやすく拡張しやすい構造にします。

    使用例:
        >>> factory = APIFactory()
        >>> handler = factory.create_weather_data_handler()
        >>> # 依存関係の注入（DIコンテナ経由）
        >>> handler.set_business_service(business_service)
    """

    @staticmethod
    def create_weather_data_handler() -> WeatherDataAPIHandler:
        """
        気象データAPIハンドラーを作成

        Returns:
            WeatherDataAPIHandler: 新しいインスタンス
        """
        return WeatherDataAPIHandler()

    @staticmethod
    def create_health_check_handler() -> BaseAPIHandler:
        """
        ヘルスチェックAPIハンドラーを作成

        Returns:
            BaseAPIHandler: ヘルスチェック用のハンドラー
        """
        return HealthCheckAPIHandler()


class HealthCheckAPIHandler(BaseAPIHandler):
    """
    ヘルスチェックAPIハンドラー

    APIサービスの稼働状態を確認するためのシンプルなハンドラーです。
    データベース接続や外部サービスとの連携状態を確認できます。

    実装上の特徴:
    - 常に正常なレスポンスを返す（バリデーションは常にTrue）
    - 実際のビジネスロジックを含まない軽量な実装
    """

    def handle_request(self, request: APIRequest) -> APIResponse:
        """
        ヘルスチェックリクエストを処理

        Args:
            request (APIRequest): ヘルスチェックリクエスト

        Returns:
            APIResponse: サービス状態情報
        """
        return APIResponse(
            status_code=200,
            data={
                "status": "healthy",
                "service": "JMA Rainfall API",
                "version": "1.0.0"  # 実際にはversion.pyから取得
            },
            timestamp=datetime.now()
        )

    def validate_request(self, request: APIRequest) -> bool:
        """
        ヘルスチェックリクエストのバリデーション

        ヘルスチェックは常に有効とするため、常にTrueを返します。

        Args:
            request (APIRequest): 検証対象のリクエスト

        Returns:
            bool: 常にTrue（ヘルスチェックは無条件で受け付ける）
        """
        return True


# 使用例とテスト例:
"""
# 基本的な使用方法
if __name__ == "__main__":
    # リクエストの作成
    request = APIRequest(
        endpoint="/weather/data",
        method="POST",
        parameters={
            "stations": [["11", "47401", "s1"]],
            "start_date": "2024-01-01",
            "end_date": "2024-01-31"
        }
    )

    # ハンドラーの作成と実行
    handler = APIFactory.create_weather_data_handler()
    response = handler.handle_request(request)

    print(f"Status: {response.status_code}")
    print(f"Data: {response.data}")
    print(f"Message: {response.message}")
"""
