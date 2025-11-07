import os
import tempfile
import time
from pathlib import Path

from src.main_datetime import (
    EmptyExcelWarning as HourlyEmptyExcelWarning,
    process_data_for_code,
)
from src.datemode import (
    EmptyExcelWarning as DailyEmptyExcelWarning,
    process_period_date_display_for_code,
)


HOURLY_CASES = [
    {
        "label": "水位 (S)",
        "code": "307051287711170",
        "mode": "S",
        "Y1": "2024",
        "M1": "1月",
        "Y2": "2024",
        "M2": "1月",
    },
    {
        "label": "流量 (R)",
        "code": "307051287711170",
        "mode": "R",
        "Y1": "2024",
        "M1": "1月",
        "Y2": "2024",
        "M2": "1月",
    },
    {
        "label": "雨量 (U)",
        "code": "102111282214010",
        "mode": "U",
        "Y1": "2024",
        "M1": "6月",
        "Y2": "2024",
        "M2": "6月",
    },
    {
        "label": "既知エラー再現 (S)",
        "code": "107111287708030",
        "mode": "S",
        "Y1": "2024",
        "M1": "1月",
        "Y2": "2024",
        "M2": "1月",
    },
]

DAILY_CASES = [
    {
        "label": "日別 水位 (S)",
        "code": "307051287711170",
        "mode": "S",
        "Y1": "2023",
        "M1": "1月",
        "Y2": "2023",
        "M2": "3月",
    },
    {
        "label": "日別 流量 (R)",
        "code": "307051287711170",
        "mode": "R",
        "Y1": "2023",
        "M1": "1月",
        "Y2": "2023",
        "M2": "3月",
    },
    {
        "label": "日別 雨量 (U)",
        "code": "102111282214010",
        "mode": "U",
        "Y1": "2023",
        "M1": "6月",
        "Y2": "2023",
        "M2": "7月",
    },
]


def _run_hourly_case(tmp_root: Path, case: dict) -> str:
    start = time.perf_counter()
    try:
        file_path = process_data_for_code(
            case["code"],
            case["Y1"],
            case["Y2"],
            case["M1"],
            case["M2"],
            case["mode"],
            single_sheet=False,
        )
        duration = time.perf_counter() - start
        return f"OK ({duration:.1f}s): {case['label']} -> {file_path}"
    except HourlyEmptyExcelWarning as err:
        duration = time.perf_counter() - start
        return f"EMPTY ({duration:.1f}s): {case['label']} -> {err}"
    except Exception as err:
        duration = time.perf_counter() - start
        return f"FAIL ({duration:.1f}s): {case['label']} -> {err}"


def _run_daily_case(tmp_root: Path, case: dict) -> str:
    start = time.perf_counter()
    try:
        file_path = process_period_date_display_for_code(
            case["code"],
            case["Y1"],
            case["Y2"],
            case["M1"],
            case["M2"],
            case["mode"],
            single_sheet=False,
        )
        duration = time.perf_counter() - start
        return f"OK ({duration:.1f}s): {case['label']} -> {file_path}"
    except DailyEmptyExcelWarning as err:
        duration = time.perf_counter() - start
        return f"EMPTY ({duration:.1f}s): {case['label']} -> {err}"
    except Exception as err:
        duration = time.perf_counter() - start
        return f"FAIL ({duration:.1f}s): {case['label']} -> {err}"


def main() -> None:
    original_cwd = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="wia-online-") as tmp_dir:
        os.chdir(tmp_dir)
        tmp_root = Path(tmp_dir)

        print(f"[Info] 作業用一時ディレクトリ: {tmp_root}")
        print("\n[時間別データ]------------------------------")
        for case in HOURLY_CASES:
            print(_run_hourly_case(tmp_root, case))

        print("\n[日別データ]--------------------------------")
        for case in DAILY_CASES:
            print(_run_daily_case(tmp_root, case))

        created_files = list(tmp_root.glob("*.xlsx"))
        if created_files:
            print("\n[生成ファイル一覧]")
            for path in created_files:
                size_kb = path.stat().st_size / 1024
                print(f"- {path.name} ({size_kb:.1f} KB)")
        else:
            print("\n[生成ファイルなし]")

        os.chdir(original_cwd)


if __name__ == "__main__":
    main()
