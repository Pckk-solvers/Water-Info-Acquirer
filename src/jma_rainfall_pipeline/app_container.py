"""依存性注入コンテナ

`WeatherDataService` や `WeatherDataAPIHandler` を組み立てるためのヘルパー関数を定義します。
GUI や API から共通のサービスを利用できるようにします。
"""

from __future__ import annotations

from datetime import timedelta
from typing import Callable, Optional

from jma_rainfall_pipeline.api.handlers.weather_data_api import WeatherDataAPIHandler
from jma_rainfall_pipeline.api.validators.request_validator import WeatherDataRequestValidator
from jma_rainfall_pipeline.domain.models import DataInterval
from jma_rainfall_pipeline.domain.services.weather_services import WeatherDataService
from jma_rainfall_pipeline.fetcher.fetcher import Fetcher
from jma_rainfall_pipeline.infrastructure.repositories.data_repository import (
    CacheService,
    JMADataRepository,
)
from jma_rainfall_pipeline.parser import parse_html
from jma_rainfall_pipeline.utils.cache_manager import CACHE_MANAGER

DEFAULT_BASE_URL = "https://www.data.jma.go.jp/"


def _default_fetcher_factory(base_url: str) -> Callable[[DataInterval], Fetcher]:
    """デフォルトのフェッチャー生成関数"""

    def factory(interval: DataInterval) -> Fetcher:
        if interval == DataInterval.DAILY:
            delta = timedelta(days=1)
        elif interval == DataInterval.MINUTE_10:
            delta = timedelta(minutes=10)
        else:
            delta = timedelta(hours=1)
        return Fetcher(base_url=base_url, interval=delta)

    return factory


def build_weather_data_service(
    *,
    base_url: str = DEFAULT_BASE_URL,
    fetcher_factory: Optional[Callable[[DataInterval], Fetcher]] = None,
    parser_func: Optional[Callable[..., "pd.DataFrame"]] = None,
) -> WeatherDataService:
    """`WeatherDataService` を構築して返す"""

    repository = JMADataRepository()
    cache_service = CacheService()
    cache_service.set_cache_manager(CACHE_MANAGER)
    repository.set_cache_service(cache_service)

    factory = fetcher_factory or _default_fetcher_factory(base_url)
    repository.set_fetcher_factory(factory)

    if parser_func is None:
        repository.set_parser(
            lambda html, freq, sample_date, obs_type="a1": parse_html(
                html,
                freq,
                sample_date,
                obs_type=obs_type,
            )
        )
    else:
        repository.set_parser(parser_func)

    service = WeatherDataService()
    service.set_data_repository(repository)
    return service


def build_weather_api_handler(
    *,
    base_url: str = DEFAULT_BASE_URL,
    fetcher_factory: Optional[Callable[[DataInterval], Fetcher]] = None,
    parser_func: Optional[Callable[..., "pd.DataFrame"]] = None,
    pretty_print: bool = False,
) -> WeatherDataAPIHandler:
    """`WeatherDataAPIHandler` を構築して返す"""

    handler = WeatherDataAPIHandler()
    service = build_weather_data_service(
        base_url=base_url,
        fetcher_factory=fetcher_factory,
        parser_func=parser_func,
    )
    handler.set_business_service(service)
    handler.set_validator(WeatherDataRequestValidator())
    handler.set_response_builder(pretty_print=pretty_print)
    return handler
