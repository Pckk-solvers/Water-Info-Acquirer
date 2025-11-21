"""気象データの取得とエクスポートを司るコントローラー。"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple
import traceback

import pandas as pd

from ..exporter.csv_exporter import export_weather_data
from ..fetcher.fetcher import Fetcher
from ..logger.app_logger import get_logger
from ..parser import parse_html
from ..utils.config_loader import get_output_directories


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
    ) -> Path:
        """
        指定された観測所リスト・期間でデータを取得しCSV/Excelへ出力する。

        :raises ValueError: 観測所が指定されていない、またはデータが空の場合
        """
        if not stations:
            raise ValueError("観測所が指定されていません")

        if output_dir is None:
            output_dirs = get_output_directories()
            output_dir = Path(output_dirs["csv_dir"])
            if excel_output_dir is None:
                excel_output_dir = Path(output_dirs["excel_dir"])
        else:
            output_dir = Path(output_dir)
            if excel_output_dir is None:
                output_dirs = get_output_directories()
                excel_output_dir = Path(output_dirs["excel_dir"])
            else:
                excel_output_dir = Path(excel_output_dir)

        interval = self.fetcher.interval
        interval_label = self._get_interval_str(interval)

        today_end = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
        if end > today_end:
            yesterday = today_end - timedelta(days=1)
            self.logger.info("取得終了日時が未来のため、前日 %s で取得を打ち切ります", yesterday.date())
            end = yesterday

        station_list = [(s[0], s[1], s[2]) for s in stations]
        self.logger.info("取得対象: 観測所数=%s, 期間=%s ～ %s", len(station_list), start, end)
        self.logger.info("観測所リスト: %s", station_list)

        results = list(self.fetcher.schedule_fetch(station_list, start, end))
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
                df = df.copy()
                dfs.append((str(prec_no), str(block_no), df))
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

        output_dir.mkdir(parents=True, exist_ok=True)

        station_dfs: Dict[Tuple[str, str], List[pd.DataFrame]] = defaultdict(list)
        for prec_value, block_value, df in dfs:
            if df.empty:
                continue
            key = (prec_value, block_value)
            station_dfs[key].append(df)

        output_paths: List[Path] = []
        for (prec_no, block_no), df_list in station_dfs.items():
            if not df_list:
                continue

            non_empty = [d for d in df_list if not d.empty]
            if not non_empty:
                self.logger.warning("観測所 %s-%s は空データのみでした", prec_no, block_no)
                continue

            merged_df = pd.concat(non_empty, ignore_index=True)
            filtered_df = self._filter_dataframe_by_range(merged_df, interval_label, start, end)
            if filtered_df.empty:
                self.logger.warning("観測所 %s-%s は指定期間内のデータがありません", prec_no, block_no)
                continue

            request_list = sorted(station_request_urls.get((prec_no, block_no), set()))
            output_path = export_weather_data(
                filtered_df,
                str(prec_no),
                str(block_no),
                interval_label,
                start.date(),
                end.date(),
                output_dir,
                export_csv=export_csv,
                export_excel=export_excel,
                excel_output_dir=excel_output_dir,
                request_urls=request_list,
            )
            output_paths.append(output_path)

        if not output_paths:
            raise ValueError("有効なファイルを出力できませんでした")

        return output_paths[0]

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
                mask = (dates >= start.date()) & (dates <= end.date())
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            if "datetime" in df.columns:
                datetimes = pd.to_datetime(df["datetime"], errors="coerce")
                mask = (datetimes >= start) & (datetimes <= end)
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            if "date" in df.columns:
                dates = pd.to_datetime(df["date"], errors="coerce").dt.date
                mask = (dates >= start.date()) & (dates <= end.date())
                return df.loc[mask.fillna(False)].reset_index(drop=True)
            return df
        except Exception as exc:  # pragma: no cover - 異常データ時の救済
            self.logger.warning("期間フィルタリングでエラーが発生しました: %s", exc)
            return df

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
