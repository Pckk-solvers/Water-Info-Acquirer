"""
APIレスポンス処理モジュール

このモジュールは、APIレスポンスのフォーマット統一と処理を担当します。
責務: レスポンスの生成、フォーマット、エラーハンドリング
"""

import json
from typing import TYPE_CHECKING, Any, Dict, Optional
from datetime import datetime

if TYPE_CHECKING:  # pragma: no cover - import for typing only
    from jma_rainfall_pipeline.api.handlers.weather_data_api import APIResponse


class ResponseFormatter:
    """レスポンスフォーマッター"""

    @staticmethod
    def format_success(data: Any, message: Optional[str] = None) -> Dict[str, Any]:
        """成功レスポンスをフォーマット"""
        return {
            "success": True,
            "data": data,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }

    @staticmethod
    def format_error(error_code: int, message: str, details: Optional[str] = None) -> Dict[str, Any]:
        """エラーレスポンスをフォーマット"""
        return {
            "success": False,
            "error": {
                "code": error_code,
                "message": message,
                "details": details
            },
            "timestamp": datetime.now().isoformat()
        }


class JSONResponseBuilder:
    """JSONレスポンスビルダー"""

    def __init__(self, pretty_print: bool = False):
        self.pretty_print = pretty_print

    def build_response(self, response: "APIResponse") -> str:
        """APIResponseからJSONレスポンスを構築"""
        if response.status_code >= 400:
            # エラーレスポンス
            formatted = ResponseFormatter.format_error(
                error_code=response.status_code,
                message=response.message or "Unknown error",
                details=str(response.data) if response.data else None
            )
        else:
            # 成功レスポンス
            formatted = ResponseFormatter.format_success(
                data=response.data,
                message=response.message
            )

        indent = 2 if self.pretty_print else None
        return json.dumps(formatted, indent=indent, ensure_ascii=False)


class CSVResponseBuilder:
    """CSVレスポンスビルダー"""

    def __init__(self):
        self._csv_exporter = None  # 後で依存性注入

    def build_response(self, response: "APIResponse") -> str:
        """APIResponseからCSVレスポンスを構築"""
        if response.status_code >= 400:
            # エラー時はJSONで返す
            formatter = JSONResponseBuilder()
            return formatter.build_response(response)

        # データがDataFrameの場合、CSVに変換
        if hasattr(response.data, 'to_csv'):
            return response.data.to_csv(index=False)

        # その他のデータタイプはJSONで返す
        formatter = JSONResponseBuilder()
        return formatter.build_response(response)


class ResponseFactory:
    """レスポンスビルダーのファクトリー"""

    @staticmethod
    def create_json_builder(pretty_print: bool = False) -> JSONResponseBuilder:
        """JSONレスポンスビルダーを作成"""
        return JSONResponseBuilder(pretty_print)

    @staticmethod
    def create_csv_builder() -> CSVResponseBuilder:
        """CSVレスポンスビルダーを作成"""
        return CSVResponseBuilder()


class HTTPStatusCodes:
    """HTTPステータスコード定義"""

    # 成功コード
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    NO_CONTENT = 204

    # リダイレクトコード
    MOVED_PERMANENTLY = 301
    FOUND = 302
    NOT_MODIFIED = 304

    # クライアントエラーコード
    BAD_REQUEST = 400
    UNAUTHORIZED = 401
    FORBIDDEN = 403
    NOT_FOUND = 404
    METHOD_NOT_ALLOWED = 405
    CONFLICT = 409
    UNPROCESSABLE_ENTITY = 422
    TOO_MANY_REQUESTS = 429

    # サーバーエラーコード
    INTERNAL_SERVER_ERROR = 500
    NOT_IMPLEMENTED = 501
    BAD_GATEWAY = 502
    SERVICE_UNAVAILABLE = 503
    GATEWAY_TIMEOUT = 504
