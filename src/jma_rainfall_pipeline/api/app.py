"""JMA 雨量パイプラインの API を FastAPI で公開するモジュール。

本モジュールは既存のドメイン/サービス層を HTTP エンドポイントへ結線し、
フロントエンド（ブラウザ/JS）から JSON 経由で同一のビジネスロジックを
利用できるようにします。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Literal, Optional

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from jma_rainfall_pipeline.api.handlers.weather_data_api import (
    APIFactory,
    APIRequest,
    APIResponse,
    BaseAPIHandler,
    WeatherDataAPIHandler,
)
from jma_rainfall_pipeline.app_container import build_weather_api_handler

DEFAULT_ALLOW_ORIGINS: Iterable[str] = ("*",)


class WeatherStationPayload(BaseModel):
    """観測所 1 件分の入力ペイロード。"""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    prefecture_code: str = Field(
        ...,
        min_length=1,
        max_length=3,
        validation_alias=AliasChoices("prefecture_code", "prefectureCode"),
        description="JMA の都道府県コード（例: '11'）。",
    )
    block_number: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("block_number", "blockNumber"),
        description="観測所の JMA ブロック番号。",
    )
    observation_type: Literal["a1", "s1"] = Field(
        ...,
        validation_alias=AliasChoices("observation_type", "observationType"),
        description="JMA の観測区分（通常 'a1' または 's1'）。",
    )
    station_name: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("station_name", "stationName"),
        description="任意の観測所名（表示用）。",
    )

    def to_tuple(self) -> tuple[str, str, str]:
        """ハンドラ層が期待する観測所識別子タプルを返却。"""

        return (self.prefecture_code, self.block_number, self.observation_type)


class WeatherDataRequestPayload(BaseModel):
    """気象データ取得のための構造化ペイロード。"""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    stations: List[WeatherStationPayload]
    start_date: datetime = Field(
        ...,
        validation_alias=AliasChoices("start_date", "startDate"),
        description="取得範囲の開始（ISO 日時または YYYY-MM-DD）。",
    )
    end_date: datetime = Field(
        ...,
        validation_alias=AliasChoices("end_date", "endDate"),
        description="取得範囲の終了（ISO 日時または YYYY-MM-DD）。",
    )
    interval: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("interval", "dataInterval"),
        description="任意のデータ間隔ヒント（例: '10min', 'hourly', 'daily'）。",
    )
    interval_minutes: Optional[int] = Field(
        None,
        validation_alias=AliasChoices("interval_minutes", "intervalMinutes"),
        description="分単位での数値的な間隔指定（代替）。",
    )
    include_metadata: Optional[bool] = Field(
        True,
        validation_alias=AliasChoices("include_metadata", "includeMetadata"),
        description="応答にサマリーメタデータを含めるか。",
    )
    output_format: Optional[str] = Field(
        "json",
        validation_alias=AliasChoices("output_format", "outputFormat"),
        description="出力形式ヒント（現状は情報目的）。",
    )
    fields: Optional[List[str]] = Field(
        None,
        validation_alias=AliasChoices("fields"),
        description="CSV 出力時に含める列リスト。",
    )

    @field_validator("fields", mode="before")
    @classmethod
    def _parse_fields(cls, value: Any):
        """fields をリストへ正規化"""
        if value is None:
            return None
        if isinstance(value, str):
            return [item.strip() for item in value.split(',') if item.strip()]
        if isinstance(value, (list, tuple, set)):
            return [str(item).strip() for item in value if str(item).strip()]
        return value

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _parse_datetime(cls, value: Any, info):
        """ISO 文字列や date をタイムゾーンなしの datetime に正規化。"""

        if isinstance(value, datetime):
            return value

        if isinstance(value, date):
            normalized = datetime.combine(value, datetime.min.time())
        elif isinstance(value, str):
            text = value.strip()
            try:
                normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(
                    f"{info.field_name} の形式が不正です。ISO 日時または YYYY-MM-DD を使用してください。"
                ) from exc
        else:
            return value

        if info.field_name == "end_date" and normalized.time() == datetime.min.time():
            # 素の日付（時刻 00:00）の終了値は当日いっぱいを含むように調整
            normalized = normalized + timedelta(days=1) - timedelta(microseconds=1)

        return normalized

    @model_validator(mode="after")
    def _validate_range(self) -> "WeatherDataRequestPayload":
        """観測所が指定され、かつ妥当な期間であることを検証。"""

        if not self.stations:
            raise ValueError("少なくとも 1 件の観測所を指定してください。")
        if self.start_date >= self.end_date:
            raise ValueError("start_date は end_date より前である必要があります。")
        return self

    def to_parameters(self) -> Dict[str, Any]:
        """ハンドラが期待する辞書形式のペイロードへ変換。"""

        params: Dict[str, Any] = {
            "stations": [station.to_tuple() for station in self.stations],
            "start_date": self.start_date,
            "end_date": self.end_date,
        }

        if self.interval is not None:
            params["interval"] = self.interval
        if self.interval_minutes is not None:
            params["interval_minutes"] = self.interval_minutes
        if self.include_metadata is not None:
            params["include_metadata"] = self.include_metadata
        if self.output_format:
            params["output_format"] = self.output_format
        if self.fields:
            params["fields"] = self.fields

        return params


def _build_http_response(api_response: APIResponse) -> Response:
    """APIResponse を FastAPI Response に変換"""

    if api_response.content_type != "application/json":
        return Response(
            content=api_response.data or "",
            media_type=api_response.content_type,
            headers=api_response.headers or {},
            status_code=api_response.status_code,
        )

    timestamp = (
        api_response.timestamp.isoformat()
        if api_response.timestamp
        else datetime.utcnow().isoformat()
    )
    payload = {
        "status": "success" if api_response.status_code < 400 else "error",
        "message": api_response.message,
        "data": api_response.data,
        "timestamp": timestamp,
    }
    return JSONResponse(
        status_code=api_response.status_code,
        content=payload,
        headers=api_response.headers or {},
    )



def create_app(
    *,
    weather_handler: Optional[WeatherDataAPIHandler] = None,
    health_handler: Optional[BaseAPIHandler] = None,
    allow_origins: Optional[Iterable[str]] = None,
) -> FastAPI:
    """FastAPI アプリケーションを生成・設定します。"""

    app = FastAPI(
        title="JMA Rainfall API",
        version=None,
        description="JMA 気象パイプライン HTTP サーバー。",
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/api/openapi.json",
    )

    origins = list(allow_origins or DEFAULT_ALLOW_ORIGINS)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["*"],
    )

    configured_weather_handler = weather_handler or build_weather_api_handler()
    configured_health_handler = health_handler or APIFactory.create_health_check_handler()

    import os

    current_dir = os.path.dirname(__file__)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    frontend_path = os.path.join(project_root, "frontend")

    @app.get("/api/health", tags=["system"])
    def health_check() -> JSONResponse:
        """起動/フロントエンド側の簡易ヘルスチェック。"""

        api_response = configured_health_handler.handle_request(
            APIRequest(endpoint="/api/health", method="GET", parameters={})
        )
        return _build_http_response(api_response)

    @app.post("/api/weather/data", tags=["weather"])
    def fetch_weather_data(payload: WeatherDataRequestPayload) -> JSONResponse:
        """指定された観測所の気象データ取得。"""

        api_request = APIRequest(
            endpoint="/api/weather/data",
            method="POST",
            parameters=payload.to_parameters(),
        )
        api_response = configured_weather_handler.handle_request(api_request)
        return _build_http_response(api_response)

    if os.path.exists(frontend_path):
        app.mount("/", StaticFiles(directory=frontend_path, html=True), name="frontend")

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover - manual entry point
    import uvicorn

    uvicorn.run("jma_rainfall_pipeline.api.app:app", host="0.0.0.0", port=8000, reload=True)
