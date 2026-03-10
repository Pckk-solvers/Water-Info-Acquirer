from __future__ import annotations

from .models import StationReport


def _meta_value_to_text(value: str | list[str]) -> str:
    if isinstance(value, list):
        return ", ".join(item for item in value if item.strip())
    return value


def _years_line(years: list[int]) -> str:
    if not years:
        return "該当なし"
    return ", ".join(str(year) for year in sorted(set(years)))


def render_markdown(report: StationReport) -> str:
    lines: list[str] = [f"# 観測所レポート {report.station_id}", ""]

    lines.append("## 観測所メタ情報")
    if report.site_meta:
        for key in sorted(report.site_meta):
            value = _meta_value_to_text(report.site_meta[key]).strip() or "N/A"
            lines.append(f"- {key}: {value}")
    else:
        lines.append("- N/A")
    lines.append("")

    lines.append("## データ登録状況（年）")
    lines.append("### 日雨量（KIND=3）")
    lines.append(_years_line(report.available_years_daily))
    lines.append("")
    lines.append("### 時間雨量（KIND=2）")
    lines.append(_years_line(report.available_years_hourly))
    lines.append("")

    if report.logs:
        error_count = sum(1 for event in report.logs if event.level == "ERROR")
        warn_count = sum(1 for event in report.logs if event.level == "WARN")
        lines.append("## 取得ログ概要")
        lines.append(f"- ERROR: {error_count}")
        lines.append(f"- WARN: {warn_count}")

    return "\n".join(lines).rstrip() + "\n"
