# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "beautifulsoup4",
#     "pdfplumber==0.11.4",
#     "requests",
# ]
# ///

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_JMA_PDF_URL = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.pdf"
DEFAULT_JMA_PDF = Path("data/source/amedas/ame_master.pdf")
DEFAULT_JMA_INDEX = Path("src/river_meta/resources/jma_station_index.json")
DEFAULT_WATERINFO_INDEX = Path("src/river_meta/resources/waterinfo_station_index.json")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JMA/WaterInfo 観測所インデックスをまとめて更新するランナー"
    )
    parser.add_argument(
        "--target",
        choices=["jma", "waterinfo", "all"],
        default="all",
        help="更新対象",
    )
    parser.add_argument(
        "--jma-mode",
        choices=["update", "rebuild"],
        default="update",
        help="JMA更新モード",
    )
    parser.add_argument(
        "--waterinfo-mode",
        choices=["resume", "rebuild"],
        default="resume",
        help="WaterInfo更新モード",
    )
    parser.add_argument(
        "--download-jma-pdf",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="JMA更新前に公式PDFを取得する",
    )
    parser.add_argument("--jma-pdf-url", default=DEFAULT_JMA_PDF_URL, help="JMA公式PDF URL")
    parser.add_argument("--jma-pdf", default=str(DEFAULT_JMA_PDF), help="JMA補完PDFパス")
    parser.add_argument("--jma-index", default=str(DEFAULT_JMA_INDEX), help="JMA JSON出力パス")
    parser.add_argument("--waterinfo-index", default=str(DEFAULT_WATERINFO_INDEX), help="WaterInfo JSON出力パス")
    parser.add_argument("--timeout", type=float, default=20.0, help="WaterInfo HTTP timeout")
    parser.add_argument("--sleep", type=float, default=0.3, help="WaterInfo request sleep(sec)")
    parser.add_argument("--max-count", type=int, default=0, help="WaterInfo取得上限(0は無制限)")
    parser.add_argument("--test-pref", type=str, default="", help="WaterInfoの都道府県絞り込み")
    parser.add_argument("--no-backup-jma", action="store_true", help="JMA更新時の .bak を作成しない")
    parser.add_argument("--keep-going", action="store_true", help="途中失敗しても可能な更新を継続")
    parser.add_argument("--dry-run", action="store_true", help="実行せずコマンドだけ表示")
    return parser.parse_args(argv)


def _run_subprocess(cmd: list[str], *, dry_run: bool) -> int:
    print("$", " ".join(cmd))
    if dry_run:
        return 0
    completed = subprocess.run(cmd, check=False)
    return completed.returncode


def _build_jma_cmd(args: argparse.Namespace, script_path: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(script_path),
        "--mode",
        args.jma_mode,
        "--in-pdf",
        args.jma_pdf,
        "--index",
        args.jma_index,
        "--output",
        args.jma_index,
    ]
    if args.no_backup_jma:
        cmd.append("--no-backup")
    return cmd


def _build_jma_pdf_download_cmd(args: argparse.Namespace, script_path: Path) -> list[str]:
    return [
        sys.executable,
        str(script_path),
        "--url",
        args.jma_pdf_url,
        "--output",
        args.jma_pdf,
    ]


def _build_waterinfo_cmd(args: argparse.Namespace, script_path: Path) -> list[str]:
    cmd = [
        sys.executable,
        str(script_path),
        "--mode",
        args.waterinfo_mode,
        "--output",
        args.waterinfo_index,
        "--timeout",
        str(args.timeout),
        "--sleep",
        str(args.sleep),
    ]
    if args.max_count > 0:
        cmd.extend(["--max-count", str(args.max_count)])
    if args.test_pref:
        cmd.extend(["--test-pref", args.test_pref])
    return cmd


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    root_dir = Path(__file__).resolve().parents[1]
    scripts_dir = root_dir / "scripts"

    download_jma_pdf_script = scripts_dir / "download_amedas_pdf.py"
    jma_script = scripts_dir / "update_jma_station_index.py"
    waterinfo_script = scripts_dir / "build_waterinfo_station_index.py"

    targets: list[str]
    if args.target == "all":
        targets = ["jma", "waterinfo"]
    else:
        targets = [args.target]

    failed = False
    for target in targets:
        if target == "jma":
            print("\n=== JMA station index refresh ===")
            if args.download_jma_pdf:
                rc = _run_subprocess(
                    _build_jma_pdf_download_cmd(args, download_jma_pdf_script),
                    dry_run=args.dry_run,
                )
                if rc != 0:
                    failed = True
                    print(f"[ERROR] jma pdf download failed (exit={rc})")
                    if not args.keep_going:
                        return rc
                    continue
            rc = _run_subprocess(_build_jma_cmd(args, jma_script), dry_run=args.dry_run)
        else:
            print("\n=== WaterInfo station index refresh ===")
            rc = _run_subprocess(_build_waterinfo_cmd(args, waterinfo_script), dry_run=args.dry_run)
        if rc != 0:
            failed = True
            print(f"[ERROR] {target} refresh failed (exit={rc})")
            if not args.keep_going:
                return rc

    if failed:
        return 1
    print("\nAll requested refresh tasks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
