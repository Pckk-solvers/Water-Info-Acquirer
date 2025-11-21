"""API 層で共通利用する定数定義。"""

CSV_ALLOWED_FIELDS: tuple[str, ...] = (
    "timestamp",
    "prefecture_code",
    "block_number",
    "station_type",
    "station_name",
    "station_id",
    "precipitation",
    "temperature",
    "humidity",
    "wind_speed",
    "wind_direction",
    "atmospheric_pressure",
    "data_quality",
    "missing_data_flags",
)

CSV_DEFAULT_FIELDS: tuple[str, ...] = (
    "timestamp",
    "prefecture_code",
    "block_number",
    "station_type",
    "precipitation",
    "temperature",
    "humidity",
    "wind_speed",
    "data_quality",
)
