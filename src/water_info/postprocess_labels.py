"""対象種別に応じた位況・流況後処理の表示定義。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Literal


MetricKey = Literal["water_level", "discharge"]


@dataclass(frozen=True, slots=True)
class MetricDefinition:
    """後処理で表示に使う対象種別の定義。"""

    key: MetricKey
    display_name: str
    statistic_name: str
    unit: str
    high_label: str
    normal_label: str
    low_label: str
    drought_label: str
    max_value_label: str
    min_value_label: str

    @property
    def status_labels(self) -> tuple[tuple[str, str], ...]:
        return (
            ("high", self.high_label),
            ("normal", self.normal_label),
            ("low", self.low_label),
            ("drought", self.drought_label),
        )


METRIC_DEFINITIONS: dict[MetricKey, MetricDefinition] = {
    "water_level": MetricDefinition(
        key="water_level",
        display_name="水位",
        statistic_name="位況",
        unit="m",
        high_label="豊水位",
        normal_label="平水位",
        low_label="低水位",
        drought_label="渇水位",
        max_value_label="最高水位",
        min_value_label="最低水位",
    ),
    "discharge": MetricDefinition(
        key="discharge",
        display_name="流量",
        statistic_name="流況",
        unit="m³/s",
        high_label="豊水流量",
        normal_label="平水流量",
        low_label="低水流量",
        drought_label="渇水流量",
        max_value_label="最大流量",
        min_value_label="最小流量",
    ),
}

_METRIC_SUFFIX_PATTERN = re.compile(r"(?:^|_)(WH|QH)(?:_|$)", re.IGNORECASE)


def normalize_metric(value: str) -> MetricKey:
    """対象種別キーを検証して返す。"""
    key = str(value).strip().lower()
    if key not in METRIC_DEFINITIONS:
        choices = ", ".join(METRIC_DEFINITIONS)
        raise ValueError(f"metricは{choices}のいずれかを指定してください: {value}")
    return key  # type: ignore[return-value]


def metric_definition(metric: str) -> MetricDefinition:
    """対象種別キーから表示定義を取得する。"""
    return METRIC_DEFINITIONS[normalize_metric(metric)]


def metric_choices() -> tuple[tuple[MetricKey, str], ...]:
    """GUIの選択肢（内部キー、表示名）を返す。"""
    return tuple(
        (definition.key, f"{definition.display_name}（{definition.statistic_name}）")
        for definition in METRIC_DEFINITIONS.values()
    )


def detect_metric_from_path(path: str | Path) -> MetricKey | None:
    """`_WH`/`_QH`のファイル名から対象種別を推定する。"""
    match = _METRIC_SUFFIX_PATTERN.search(Path(path).stem)
    if match is None:
        return None
    return "water_level" if match.group(1).upper() == "WH" else "discharge"


def build_output_label_maps(metric: str, *, include_daily: bool) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Excelのmain/peaks/summary用の表示ラベルを生成する。"""
    definition = metric_definition(metric)
    value_label = definition.display_name
    daily_label = f"日データ（{value_label}）"
    denominator_labels = {
        "var_den": "可変分母",
        "fixed_den": "固定分母",
        "daily_value": daily_label,
    }

    main_map = {
        "hydro_date": "日付",
        "year": "年",
        "hourly_daily_avg_var_den": f"日平均{value_label}（可変分母）",
        "hourly_daily_avg_fixed_den": f"日平均{value_label}（固定分母）",
        "daily_value": daily_label,
        "count_non_null": "非欠損本数",
        "rank_var_den": "ランク（可変分母）",
        "rank_fixed_den": "ランク（固定分母）",
        "rank_daily_value": f"ランク（{daily_label}）",
        "rank_var_den_ref": "ランク（可変分母,参考）",
        "rank_fixed_den_ref": "ランク（固定分母,参考）",
        "rank_daily_value_ref": f"ランク（{daily_label},参考）",
    }

    peak_map = {
        "hydro_date": "日付",
        "peak_max_value": definition.max_value_label,
        "peak_max_time": f"{definition.max_value_label}発生日時（水文水質DB基準）",
    }

    year_map = {
        "year": "年",
        "missing_var_den": "欠損数（可変分母）",
        "missing_fixed_den": "欠損数（固定分母）",
        "missing_daily_value": f"欠損数（{daily_label}）",
        "mean_var_den": f"平均{value_label}（可変分母）",
        "mean_fixed_den": f"平均{value_label}（固定分母）",
        "mean_daily_value": f"平均{daily_label}",
        "max_hourly_value": f"{definition.max_value_label}（1時間値）",
        "max_hourly_time": f"{definition.max_value_label}発生日時（水文水質DB基準）",
        "min_hourly_value": f"{definition.min_value_label}（1時間値）",
        "min_hourly_time": f"{definition.min_value_label}発生日時（水文水質DB基準）",
    }

    for level_key, status_label in definition.status_labels:
        for suffix, suffix_label in denominator_labels.items():
            rank_key = f"rank_used_ikyo_{level_key}_{suffix}"
            ikyo_key = f"ikyo_{level_key}_{suffix}"
            year_map[rank_key] = f"採用順位（{status_label},{suffix_label}）"
            year_map[ikyo_key] = f"{definition.statistic_name}{status_label}（{suffix_label}）"

    if not include_daily:
        for key in ("daily_value", "rank_daily_value", "rank_daily_value_ref"):
            main_map.pop(key, None)
        for key in ("missing_daily_value", "mean_daily_value"):
            year_map.pop(key, None)
        for level_key, _status_label in definition.status_labels:
            year_map.pop(f"rank_used_ikyo_{level_key}_daily_value", None)
            year_map.pop(f"ikyo_{level_key}_daily_value", None)

    return main_map, peak_map, year_map
