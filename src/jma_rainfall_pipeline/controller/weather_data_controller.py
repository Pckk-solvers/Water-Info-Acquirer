"""気象データの取得とエクスポートを司るコントローラー。"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
import traceback

import pandas as pd

from ..exporter.csv_exporter import export_weather_data
from ..exporter.parquet_exporter import build_normalized_time_frame, export_weather_parquet
from ..fetcher.fetcher import Fetcher
from ..logger.app_logger import get_logger
from ..parser import parse_html
from ..utils.config_loader import get_output_directories


@dataclass(frozen=True)
class StationExportResult:
    prec_no: str
    block_no: str
    interval_label: str
    csv_path: Path | None
    excel_path: Path | None
    parquet_path: Path | None
    request_urls: tuple[str, ...]


@dataclass(frozen=True)
class WeatherExportSummary:
    results: tuple[StationExportResult, ...]


class WeatherDataController:
    """観測データの取得・加工・出力を統括するコントローラー。"""

    logger = get_logger(__name__)

    _NO_DATA_MARKERS = (
        "データは存在しません",
        "データはありません",
        "データが見つかりません",
    )

    def __init__(self, base_url: str = "https://www.data.jma.go.jp/", interval: timedelta | None = None) -> None:
        """
        コントローラーを初期化する。

        :param base_url: 気象庁データサイトのベースURL
        :param interval: データ取得間隔（未指定時は1時間）
        """
        self.fetcher = Fetcher(base_url=base_url, interval=interval or timedelta(hours=1))

    def fetch_and_export_data(
        self,
        stations: List[Tuple[str, str, str]],
        start: datetime,
        end: datetime,
        output_dir: Path | None = None,
        export_csv: bool = True,
        export_excel: bool = True,
        excel_output_dir: Path | None = None,
        export_parquet: bool = False,
        parquet_output_dir: Path | None = None,
    ) -> Path:
        """
        指定された観測所リスト・期間でデータを取得しCSV/Excelへ出力する。

        :raises ValueError: 観測所が指定されていない、またはデータが空の場合
        """
        summary = self.fetch_and_export_summary(
            stations=stations,
            start=start,
            end=end,
            output_dir=output_dir,
            export_csv=export_csv,
            export_excel=export_excel,
            excel_output_dir=excel_output_dir,
            export_parquet=export_parquet,
            parquet_output_dir=parquet_output_dir,
        )
        if not summary.results:
            raise ValueError("有効なファイルを出力できませんでした")
        first = summary.results[0]
        if first.csv_path is not None:
            return first.csv_path
        if first.excel_path is not None:
            return first.excel_path
        if first.parquet_path is not None:
            return first.parquet_path
        raise ValueError("有効なファイルを出力できませんでした")

    def fetch_and_export_summary(
        self,
        stations: List[Tuple[str, str, str]],
        start: datetime,
        end: datetime,
        output_dir: Path | None = None,
        export_csv: bool = True,
        export_excel: bool = True,
        excel_output_dir: Path | None = None,
        export_parquet: bool = False,
        parquet_output_dir: Path | None = None,
    ) -> WeatherExportSummary:
        """指定された観測所リスト・期間でデータを取得し出力結果を返す。

        `end` は排他的上限として扱う。
        """
        if not stations:
            raise ValueError("観測所が指定されていません")

        csv_dir, resolved_excel_dir, resolved_parquet_dir = self._resolve_output_directories(
            output_dir=output_dir,
            excel_output_dir=excel_output_dir,
            parquet_output_dir=parquet_output_dir,
        )
        interval = self.fetcher.interval
        interval_label = self._get_interval_str(interval)
        public_end_date = self._public_end_date(end)

        today_exclusive = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        if end > today_exclusive:
            capped_public_end_date = self._public_end_date(today_exclusive)
            self.logger.info("取得終了日時が未来のため、%s で取得を打ち切ります", capped_public_end_date)
            end = today_exclusive
            public_end_date = capped_public_end_date

        station_list: list[tuple[str, str, str | None]] = [(s[0], s[1], s[2]) for s in stations]
        self.logger.info("取得対象: 観測所数=%s, 期間=%s ～ %s", len(station_list), start, public_end_date)
        self.logger.info("観測所リスト: %s", station_list)

        fetch_start = start
        fetch_end = self._schedule_fetch_end(end)
        if interval < timedelta(days=1):
            fetch_start = start - interval
            self.logger.info(
                "hourly/10min の境界補完のため、内部取得開始を拡張: %s -> %s",
                start,
                fetch_start,
            )

        results = list(self.fetcher.schedule_fetch(station_list, fetch_start, fetch_end))
        if not results:
            raise ValueError("有効なデータが取得できませんでした。観測所や日時を確認してください。")
        self.logger.info("取得件数: %s レコード", len(results))

        dfs: List[tuple[str, str, pd.DataFrame]] = []
        station_request_urls: Dict[Tuple[str, str], set[str]] = defaultdict(set)
        freq_label = self._get_frequency(interval)

        for idx, ((prec_no, block_no), dt, html, url) in enumerate(results, 1):
            self.logger.info(
                "[%s/%s] 解析対象: prec_no=%s, block_no=%s, dt=%s, URL=%s, HTMLサイズ=%s bytes",
                idx,
                len(results),
                prec_no,
                block_no,
                dt,
                url,
                len(html),
            )
            station_request_urls[(str(prec_no), str(block_no))].add(url)

            if self._contains_no_data_marker(html):
                self.logger.info("データ未掲載のためスキップ: %s", dt)
                continue

            try:
                obs_type = self._normalize_obs_type(stations, prec_no, block_no)
                df = parse_html(html, freq_label, dt.date(), obs_type=obs_type)
                if df.empty:
                    self.logger.warning("解析結果が空のためスキップ")
                    continue
                dfs.append((str(prec_no), str(block_no), df.copy()))
            except Exception as exc:  # pragma: no cover - 解析失敗時の救済
                self.logger.warning(
                    "パースエラー (prec_no=%s, block_no=%s, dt=%s): %s",
                    prec_no,
                    block_no,
                    dt,
                    exc,
                )
                traceback.print_exc()
                continue

        if not dfs:
            raise ValueError("解析可能なデータが得られませんでした")

        if export_csv:
            csv_dir.mkdir(parents=True, exist_ok=True)
        if export_parquet:
            resolved_parquet_dir.mkdir(parents=True, exist_ok=True)

        station_dfs: Dict[Tuple[str, str], List[pd.DataFrame]] = defaultdict(list)
        for prec_value, block_value, df in dfs:
            if df.empty:
                continue
            station_dfs[(prec_value, block_value)].append(df)

        exported: list[StationExportResult] = []
        for (prec_no, block_no), df_list in station_dfs.items():
            mergeable = self._collect_mergeable_frames(df_list)
            if not mergeable:
                self.logger.warning("観測所 %s-%s は空データのみでした", prec_no, block_no)
                continue

            merged_df = pd.concat(mergeable, ignore_index=True)
            filtered_df = self._filter_dataframe_by_range(merged_df, interval_label, start, end)
            if filtered_df.empty:
                self.logger.warning("観測所 %s-%s は指定期間内のデータがありません", prec_no, block_no)
                continue

            request_list = tuple(sorted(station_request_urls.get((prec_no, block_no), set())))
            output_path = export_weather_data(
                filtered_df,
                str(prec_no),
                str(block_no),
                interval_label,
                start.date(),
                public_end_date,
                csv_dir,
                export_csv=export_csv,
                export_excel=export_excel,
                excel_output_dir=resolved_excel_dir,
                request_urls=list(request_list),
            )
            csv_path = output_path if export_csv else None
            excel_path = None
            if export_excel:
                excel_path = self._resolve_jma_excel_path(
                    csv_path=output_path,
                    csv_dir=csv_dir,
                    excel_output_dir=resolved_excel_dir,
                )
            parquet_path = None
            if export_parquet:
                parquet_df = filtered_df.copy()
                if parquet_df.empty:
                    self.logger.warning(
                        "観測所 %s-%s はParquet対象期間内のデータがありません",
                        prec_no,
                        block_no,
                    )
                else:
                    parquet_path = export_weather_parquet(
                        parquet_df,
                        prec_no=str(prec_no),
                        block_no=str(block_no),
                        interval_label=interval_label,
                        start_date=start.date(),
                        end_date=public_end_date,
                        output_dir=resolved_parquet_dir,
                    )
            exported.append(
                StationExportResult(
                    prec_no=str(prec_no),
                    block_no=str(block_no),
                    interval_label=interval_label,
                    csv_path=csv_path,
                    excel_path=excel_path,
                    parquet_path=parquet_path,
                    request_urls=request_list,
                )
            )

        return WeatherExportSummary(results=tuple(exported))

    def _resolve_output_directories(
        self,
        *,
        output_dir: Path | None,
        excel_output_dir: Path | None,
        parquet_output_dir: Path | None,
    ) -> tuple[Path, Path, Path]:
        if output_dir is None:
            output_dirs = get_output_directories()
            csv_dir = Path(output_dirs["csv_dir"])
            resolved_excel_dir = (
                Path(output_dirs["excel_dir"]) if excel_output_dir is None else Path(excel_output_dir)
            )
            resolved_parquet_dir = (
                Path(output_dirs["parquet_dir"])
                if parquet_output_dir is None
                else Path(parquet_output_dir)
            )
            return csv_dir, resolved_excel_dir, resolved_parquet_dir

        base_dir = Path(output_dir)
        resolved_excel_dir = Path(excel_output_dir) if excel_output_dir is not None else base_dir / "excel"
        resolved_parquet_dir = (
            Path(parquet_output_dir) if parquet_output_dir is not None else base_dir / "parquet"
        )
        return base_dir / "csv", resolved_excel_dir, resolved_parquet_dir

    def _resolve_jma_excel_path(
        self,
        *,
        csv_path: Path,
        csv_dir: Path,
        excel_output_dir: Path,
    ) -> Path:
        file_name = csv_path.name.replace(".csv", ".xlsx")
        if csv_path.parent == excel_output_dir:
            return csv_path.with_suffix(".xlsx")
        if csv_path.parent == csv_dir:
            return excel_output_dir / file_name
        return csv_path.parent / file_name

    def _normalize_obs_type(self, stations: List[Tuple[str, str, str]], prec_no: str, block_no: str) -> str:
        """駅種別指定の揺れを補正する。"""
        obs_type = next((s[2] for s in stations if s[0] == prec_no and s[1] == block_no), "a1")
        obs_type = (obs_type or "a1").strip().lower()
        if obs_type == "a":
            return "a1"
        if obs_type == "s":
            return "s1"
        if not obs_type.endswith("1"):
            return f"{obs_type}1"
        return obs_type

    def _contains_no_data_marker(self, html: str) -> bool:
        """HTML内に「データ無し」を示す文言が含まれるか確認する。"""
        return any(marker in html for marker in self._NO_DATA_MARKERS)

    def _filter_dataframe_by_range(
        self, df: pd.DataFrame, interval_label: str, start: datetime, end: datetime
    ) -> pd.DataFrame:
        """取得期間に合わせてDataFrameをフィルタリングする。"""
        if df.empty:
            return df
        try:
            if interval_label == "daily" and "date" in df.columns:
                dates = pd.to_datetime(df["date"], errors="coerce").dt.date
                mask = (dates >= start.date()) & (dates <= self._public_end_date(end))
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            if interval_label in {"hourly", "10min"}:
                normalized = build_normalized_time_frame(df, interval_label)
                period_end = pd.to_datetime(normalized["period_end_at"], errors="coerce")
                mask = (period_end >= start) & (period_end <= end)
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            if "datetime" in df.columns:
                datetimes = pd.to_datetime(df["datetime"], errors="coerce")
                mask = (datetimes >= start) & (datetimes <= end)
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            if "date" in df.columns:
                dates = pd.to_datetime(df["date"], errors="coerce").dt.date
                mask = (dates >= start.date()) & (dates <= self._public_end_date(end))
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            return df
        except Exception as exc:  # pragma: no cover - 異常データ時の救済
            self.logger.warning("期間フィルタリングでエラーが発生しました: %s", exc)
            return df

    def _public_end_date(self, end: datetime):
        """排他的上限から利用者向けの最終日を返す。"""

        if end.time() == datetime.min.time():
            return (end - timedelta(days=1)).date()
        return end.date()

    def _schedule_fetch_end(self, end: datetime) -> datetime:
        """schedule_fetch に渡す最終取得日を返す。"""

        if end.time() == datetime.min.time():
            return end - timedelta(days=1)
        return end

    def _collect_mergeable_frames(self, df_list: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """concat 対象から空または全NAのフレームを除外する。"""

        mergeable: list[pd.DataFrame] = []
        for frame in df_list:
            if frame.empty:
                continue
            cleaned = frame.dropna(how="all").dropna(axis=1, how="all")
            if cleaned.empty:
                continue
            mergeable.append(cleaned)
        return mergeable

    def _determine_interval(self, start: datetime, end: datetime) -> timedelta:
        """期間に合わせたおすすめの間隔を返す。"""
        delta = end - start
        if delta <= timedelta(days=1):
            return timedelta(minutes=10)
        if delta <= timedelta(weeks=1):
            return timedelta(hours=1)
        return timedelta(days=1)

    def _get_frequency(self, interval: timedelta) -> str:
        """間隔からJMAのfrequency文字列を決定する。"""
        if interval >= timedelta(days=1):
            return "daily"
        if interval >= timedelta(hours=1):
            return "hourly"
        return "10min"

    def _get_interval_str(self, interval: timedelta) -> str:
        """ラベル用にintervalを文字列化する。"""
        return self._get_frequency(interval)
