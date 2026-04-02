"""Generate many parquet inputs for graph-development experiments.

Usage:
    uv run python scripts/generate_parquet_input_samples.py
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class GeneratedDataset:
    source: str
    mode: str
    interval: str
    station_id: str
    station_name: str
    period_start: str
    period_end: str
    parquet_path: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _run(cmd: list[str], *, cwd: Path, timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )


def _copy_new_parquet(temp_dir: Path, output_root: Path) -> list[Path]:
    copied: list[Path] = []
    for path in sorted(temp_dir.rglob("*.parquet")):
        target = output_root / path.name
        if target.exists():
            continue
        shutil.copy2(path, target)
        copied.append(target)
    return copied


def _random_jma_period(rng: random.Random, interval: str) -> tuple[str, str]:
    if interval == "daily":
        start_base = date(2025, 1, 1)
        span_days = 420
        length_days = rng.randint(7, 25)
    elif interval == "hourly":
        start_base = date(2025, 9, 1)
        span_days = 190
        length_days = rng.randint(2, 5)
    else:
        start_base = date(2025, 12, 1)
        span_days = 120
        length_days = rng.randint(2, 4)
    start = start_base + timedelta(days=rng.randint(0, span_days))
    end = start + timedelta(days=length_days - 1)
    return start.isoformat(), end.isoformat()


def _month_token(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def _random_water_period(rng: random.Random, mode: str, interval: str) -> tuple[str, str]:
    if mode == "S":
        candidates = [_month_token(2024, 11), _month_token(2024, 12), _month_token(2025, 1), _month_token(2025, 2)]
    elif mode == "R":
        candidates = [_month_token(2024, 5), _month_token(2024, 6), _month_token(2024, 7), _month_token(2024, 8)]
    else:
        candidates = [_month_token(2024, 5), _month_token(2024, 6), _month_token(2024, 7)]

    start = rng.choice(candidates)
    if interval == "daily" and rng.random() < 0.35:
        start_year, start_month = map(int, start.split("-"))
        if start_month < 12:
            end = _month_token(start_year, start_month + 1)
        else:
            end = _month_token(start_year + 1, 1)
        return start, end
    return start, start


def _collect_water_station_ids(repo: Path, pref_names: list[str], out_csv: Path) -> list[str]:
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "river_meta.station_ids.cli",
        "--item",
        "水位流量",
        "--out",
        str(out_csv.with_suffix(".txt")),
        "--out-pref-csv",
        str(out_csv),
    ]
    for pref in pref_names:
        cmd.extend(["--pref", pref])
    proc = _run(cmd, cwd=repo, timeout=600)
    if proc.returncode != 0:
        raise RuntimeError(f"station id collection failed: {proc.stdout}\n{proc.stderr}")
    ids: list[str] = []
    with out_csv.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            station_id = row["station_id"].strip()
            if station_id and station_id not in ids:
                ids.append(station_id)
    return ids


def _load_water_rainfall_ids(repo: Path) -> list[str]:
    data = _load_json(repo / "src" / "river_meta" / "resources" / "waterinfo_station_index.json")
    by_station = data["by_station_id"]
    return sorted(by_station.keys())


def _load_jma_candidates(repo: Path) -> list[dict[str, str]]:
    data = _load_json(repo / "src" / "river_meta" / "resources" / "jma_station_index.json")
    by_block = data["by_block_no"]
    candidates: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for records in by_block.values():
        for rec in records:
            obs_type = rec.get("obs_type") or ""
            key = (str(rec.get("prec_no", "")), str(rec.get("block_no", "")), obs_type)
            if not key[0] or not key[1] or obs_type not in {"a1", "s1"} or key in seen:
                continue
            seen.add(key)
            candidates.append(
                {
                    "prec_no": key[0],
                    "block_no": key[1],
                    "obs_type": obs_type,
                    "station_name": str(rec.get("station_name", "")).strip(),
                }
            )
    return candidates


def _parse_station_name_from_stdout(stdout: str) -> str:
    for line in stdout.splitlines():
        text = line.strip()
        if text.startswith("{") and '"station_name"' in text:
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            station_name = str(payload.get("station_name", "")).strip()
            if station_name:
                return station_name
    return ""


def _load_station_name_from_parquet(parquet_path: Path) -> str:
    df = pd.read_parquet(parquet_path, columns=["station_name"])
    if df.empty or "station_name" not in df.columns:
        return ""
    return str(df["station_name"].iloc[0]).strip()


def _generate_jma(
    *,
    repo: Path,
    output_root: Path,
    temp_root: Path,
    rng: random.Random,
    candidates: list[dict[str, str]],
    interval: str,
    target_count: int,
) -> list[GeneratedDataset]:
    generated: list[GeneratedDataset] = []
    used_keys: set[tuple[str, str, str, str, str]] = set()
    attempts = 0
    max_attempts = target_count * 12
    while len(generated) < target_count and attempts < max_attempts:
        attempts += 1
        station = rng.choice(candidates)
        start, end = _random_jma_period(rng, interval)
        key = (station["prec_no"], station["block_no"], interval, start, end)
        if key in used_keys:
            continue
        used_keys.add(key)
        run_dir = temp_root / f"jma_{interval}_{attempts:03d}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        cmd = [
            sys.executable,
            "-m",
            "jma_rainfall_pipeline",
            "fetch",
            "--station",
            f"{station['prec_no']}:{station['block_no']}:{station['obs_type']}",
            "--start",
            start,
            "--end",
            end,
            "--interval",
            interval,
            "--parquet",
            "--no-excel",
            "--output-dir",
            str(run_dir),
        ]
        proc = _run(cmd, cwd=repo, timeout=420)
        if proc.returncode != 0:
            continue
        copied = _copy_new_parquet(run_dir, output_root)
        if not copied:
            continue
        for parquet_path in copied:
            station_name = _load_station_name_from_parquet(parquet_path) or station["station_name"] or _parse_station_name_from_stdout(proc.stdout)
            generated.append(
                GeneratedDataset(
                    source="jma",
                    mode="rainfall",
                    interval=interval,
                    station_id=f"{station['prec_no']}_{station['block_no']}",
                    station_name=station_name,
                    period_start=start,
                    period_end=end,
                    parquet_path=str(parquet_path.relative_to(repo)),
                )
            )
    return generated


def _generate_water_info(
    *,
    repo: Path,
    output_root: Path,
    temp_root: Path,
    rng: random.Random,
    candidates: list[str],
    mode: str,
    interval: str,
    target_count: int,
) -> list[GeneratedDataset]:
    generated: list[GeneratedDataset] = []
    used_keys: set[tuple[str, str, str, str, str]] = set()
    attempts = 0
    max_attempts = target_count * 20
    while len(generated) < target_count and attempts < max_attempts:
        attempts += 1
        code = rng.choice(candidates)
        start, end = _random_water_period(rng, mode, interval)
        key = (code, mode, interval, start, end)
        if key in used_keys:
            continue
        used_keys.add(key)
        run_dir = temp_root / f"water_{mode}_{interval}_{attempts:03d}"
        if run_dir.exists():
            shutil.rmtree(run_dir)
        cmd = [
            sys.executable,
            "-m",
            "water_info",
            "fetch",
            "--code",
            code,
            "--mode",
            mode,
            "--start",
            start,
            "--end",
            end,
            "--interval",
            interval,
            "--parquet",
            "--no-excel",
            "--output-dir",
            str(run_dir),
        ]
        proc = _run(cmd, cwd=repo, timeout=420)
        if proc.returncode != 0:
            continue
        copied = _copy_new_parquet(run_dir, output_root)
        if not copied:
            continue
        for parquet_path in copied:
            station_name = _load_station_name_from_parquet(parquet_path) or _parse_station_name_from_stdout(proc.stdout)
            generated.append(
                GeneratedDataset(
                    source="water_info",
                    mode=mode,
                    interval=interval,
                    station_id=code,
                    station_name=station_name,
                    period_start=start,
                    period_end=end,
                    parquet_path=str(parquet_path.relative_to(repo)),
                )
            )
    return generated


def main() -> int:
    parser = argparse.ArgumentParser(description="data/parquet_input にグラフ検証用 Parquet を量産する")
    parser.add_argument("--output-root", default="data/parquet_input")
    parser.add_argument("--temp-root", default="tmp/parquet_input_build")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    repo = _repo_root()
    output_root = (repo / args.output_root).resolve()
    temp_root = (repo / args.temp_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    temp_root.mkdir(parents=True, exist_ok=True)

    seed = args.seed if args.seed is not None else random.SystemRandom().randrange(1, 2**31)
    rng = random.Random(seed)

    water_pref_pool = ["北海道", "東京都", "埼玉県", "茨城県", "千葉県", "大阪府", "兵庫県"]
    rng.shuffle(water_pref_pool)
    pref_selection = water_pref_pool[:5]
    water_level_flow_ids = _collect_water_station_ids(repo, pref_selection, temp_root / "water_level_flow_ids.csv")
    water_rain_ids = _load_water_rainfall_ids(repo)
    jma_candidates = _load_jma_candidates(repo)
    rng.shuffle(water_level_flow_ids)
    rng.shuffle(water_rain_ids)
    rng.shuffle(jma_candidates)

    all_generated: list[GeneratedDataset] = []
    all_generated.extend(
        _generate_jma(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=jma_candidates,
            interval="daily",
            target_count=4,
        )
    )
    all_generated.extend(
        _generate_jma(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=jma_candidates,
            interval="hourly",
            target_count=4,
        )
    )
    all_generated.extend(
        _generate_jma(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=jma_candidates,
            interval="10min",
            target_count=4,
        )
    )
    all_generated.extend(
        _generate_water_info(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=water_level_flow_ids,
            mode="S",
            interval="hourly",
            target_count=3,
        )
    )
    all_generated.extend(
        _generate_water_info(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=water_level_flow_ids,
            mode="R",
            interval="hourly",
            target_count=2,
        )
    )
    all_generated.extend(
        _generate_water_info(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=water_level_flow_ids,
            mode="R",
            interval="daily",
            target_count=2,
        )
    )
    all_generated.extend(
        _generate_water_info(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=water_rain_ids,
            mode="U",
            interval="hourly",
            target_count=2,
        )
    )
    all_generated.extend(
        _generate_water_info(
            repo=repo,
            output_root=output_root,
            temp_root=temp_root,
            rng=rng,
            candidates=water_rain_ids,
            mode="U",
            interval="daily",
            target_count=2,
        )
    )

    summary = {
        "seed": seed,
        "output_root": str(output_root.relative_to(repo)),
        "dataset_count": len(all_generated),
        "datasets": [asdict(item) for item in all_generated],
    }
    summary_path = output_root / "_generation_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    counts: dict[str, int] = {}
    for item in all_generated:
        key = f"{item.source}:{item.mode}:{item.interval}"
        counts[key] = counts.get(key, 0) + 1

    print(json.dumps({"seed": seed, "dataset_count": len(all_generated), "counts": counts}, ensure_ascii=False))
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
