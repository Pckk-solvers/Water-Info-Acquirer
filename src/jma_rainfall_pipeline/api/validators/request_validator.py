"""
API入力バリデーションモジュール

このモジュールは、APIリクエストの入力バリデーションを担当します。
責務: リクエストパラメータの検証とサニタイズ
"""

import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from datetime import datetime, date
from dataclasses import dataclass

from jma_rainfall_pipeline.api.constants import CSV_ALLOWED_FIELDS

if TYPE_CHECKING:  # pragma: no cover - typing only
    from jma_rainfall_pipeline.api.handlers.weather_data_api import APIRequest


@dataclass
class ValidationResult:
    """バリデーション結果"""
    is_valid: bool
    errors: List[str]
    sanitized_data: Optional[Dict[str, Any]] = None


class BaseValidator:
    """バリデータの基底クラス"""

    def validate(self, data: Any) -> ValidationResult:
        """データをバリデーションする"""
        raise NotImplementedError

    def sanitize(self, data: Any) -> Any:
        """データをサニタイズする"""
        return data


class WeatherDataRequestValidator(BaseValidator):
    """気象データリクエストバリデータ"""

    def __init__(self):
        self._required_fields = ['stations', 'start_date', 'end_date']
        self._optional_fields = ['include_metadata', 'interval_minutes']

    def validate(self, request: "APIRequest") -> ValidationResult:
        """気象データリクエストをバリデーション"""
        errors = []

        # 必須フィールドのチェック
        for field in self._required_fields:
            if field not in request.parameters:
                errors.append(f"Missing required field: {field}")

        if errors:
            return ValidationResult(is_valid=False, errors=errors)

        # データ型と値の検証
        sanitized_data = {}

        # 観測所の検証
        stations_result = self._validate_stations(request.parameters['stations'])
        if not stations_result.is_valid:
            errors.extend(stations_result.errors)
        else:
            sanitized_data['stations'] = stations_result.sanitized_data

        # 日付の検証
        start_date_result = self._validate_date(request.parameters['start_date'], 'start_date')
        if not start_date_result.is_valid:
            errors.extend(start_date_result.errors)
        else:
            sanitized_data['start_date'] = start_date_result.sanitized_data

        end_date_result = self._validate_date(request.parameters['end_date'], 'end_date')
        if not end_date_result.is_valid:
            errors.extend(end_date_result.errors)
        else:
            sanitized_data['end_date'] = end_date_result.sanitized_data

        # 日付範囲の検証
        if start_date_result.is_valid and end_date_result.is_valid:
            if sanitized_data['start_date'] >= sanitized_data['end_date']:
                errors.append("Start date must be before end date")

        # output_format / fields の検証
        output_format = str(request.parameters.get('output_format', 'json')).lower()
        if output_format not in {'json', 'csv'}:
            errors.append("Invalid output_format. Supported values are 'json' or 'csv'")
        sanitized_data['output_format'] = output_format

        if 'fields' in request.parameters:
            fields_result = self._validate_fields(request.parameters['fields'])
            if not fields_result.is_valid:
                errors.extend(fields_result.errors)
            elif output_format != 'csv':
                errors.append("fields parameter is only allowed when output_format is 'csv'")
            else:
                sanitized_data['fields'] = fields_result.sanitized_data
        elif output_format == 'csv':
            sanitized_data['fields'] = None

        # �I�v�V�������ڂ̌���
        for field in self._optional_fields:
            if field in request.parameters:
                sanitized_data[field] = request.parameters[field]

        if errors:
            return ValidationResult(is_valid=False, errors=errors)

        return ValidationResult(
            is_valid=True,
            errors=[],
            sanitized_data=sanitized_data
        )

    def _validate_stations(self, stations: Any) -> ValidationResult:
        """観測所データを検証"""
        errors = []

        if not isinstance(stations, list):
            return ValidationResult(
                is_valid=False,
                errors=["Stations must be a list"]
            )

        sanitized_stations = []
        for i, station in enumerate(stations):
            if not isinstance(station, (list, tuple)) or len(station) != 3:
                errors.append(f"Station {i} must be a tuple of (pref_code, block_no, obs_type)")
                continue

            pref_code, block_no, obs_type = station

            # 都道府県コードの検証（2桁の数字）
            if not re.match(r'^\d{2}$', str(pref_code)):
                errors.append(f"Station {i}: Invalid prefecture code")

            # ブロック番号の検証（数字）
            if not str(block_no).isdigit():
                errors.append(f"Station {i}: Invalid block number")

            # 観測所タイプの検証（a1またはs1）
            if obs_type not in ['a1', 's1']:
                errors.append(f"Station {i}: Invalid observation type")

            sanitized_stations.append((pref_code, block_no, obs_type))

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            sanitized_data=sanitized_stations
        )
    def _validate_fields(self, fields: Any) -> ValidationResult:
        """CSV出力向け fields パラメータの検証"""
        errors: List[str] = []

        if isinstance(fields, str):
            candidates = [item.strip() for item in fields.split(',') if item.strip()]
        elif isinstance(fields, (list, tuple, set)):
            candidates = [str(item).strip() for item in fields if str(item).strip()]
        else:
            errors.append("fields must be a string or list of strings")
            return ValidationResult(is_valid=False, errors=errors)

        if not candidates:
            errors.append("fields cannot be empty")
            return ValidationResult(is_valid=False, errors=errors)

        invalid = [field for field in candidates if field not in CSV_ALLOWED_FIELDS]
        if invalid:
            errors.append(f"Invalid fields: {', '.join(invalid)}")

        # 重複排除（指定順序は維持）
        sanitized: List[str] = []
        for candidate in candidates:
            if candidate not in sanitized:
                sanitized.append(candidate)

        if errors:
            return ValidationResult(is_valid=False, errors=errors)

        return ValidationResult(is_valid=True, errors=[], sanitized_data=sanitized)



    def _validate_date(self, date_value: Any, field_name: str) -> ValidationResult:
        """日付データを検証"""
        errors = []

        if isinstance(date_value, str):
            try:
                # ISO形式またはYYYY-MM-DD形式を試行
                if 'T' in date_value:
                    parsed_date = datetime.fromisoformat(date_value.replace('Z', '+00:00'))
                else:
                    parsed_date = datetime.strptime(date_value, '%Y-%m-%d')
            except ValueError:
                errors.append(f"Invalid {field_name} format. Use YYYY-MM-DD or ISO format")
                return ValidationResult(is_valid=False, errors=errors)
        elif isinstance(date_value, (datetime, date)):
            parsed_date = date_value
        else:
            errors.append(f"{field_name} must be a string or datetime object")
            return ValidationResult(is_valid=False, errors=errors)

        # 日付範囲の検証（過去10年以内、未来1年以内）
        now = datetime.now()
        ten_years_ago = now.replace(year=now.year - 10)
        one_year_later = now.replace(year=now.year + 1)

        if parsed_date < ten_years_ago:
            errors.append(f"{field_name} cannot be more than 10 years in the past")
        elif parsed_date > one_year_later:
            errors.append(f"{field_name} cannot be more than 1 year in the future")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            sanitized_data=parsed_date
        )


class PaginationValidator(BaseValidator):
    """ページネーションバリデータ"""

    def __init__(self):
        self._max_page_size = 1000
        self._max_page = 10000

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """ページネーションパラメータを検証"""
        errors = []

        if 'page' in data:
            page = data['page']
            if not isinstance(page, int) or page < 1:
                errors.append("Page must be a positive integer")
            elif page > self._max_page:
                errors.append(f"Page cannot exceed {self._max_page}")

        if 'page_size' in data:
            page_size = data['page_size']
            if not isinstance(page_size, int) or page_size < 1:
                errors.append("Page size must be a positive integer")
            elif page_size > self._max_page_size:
                errors.append(f"Page size cannot exceed {self._max_page_size}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            sanitized_data=data if len(errors) == 0 else None
        )


class ValidationFactory:
    """バリデータファクトリー"""

    @staticmethod
    def create_weather_data_validator() -> WeatherDataRequestValidator:
        """気象データバリデータを作成"""
        return WeatherDataRequestValidator()

    @staticmethod
    def create_pagination_validator() -> PaginationValidator:
        """ページネーションバリデータを作成"""
        return PaginationValidator()
