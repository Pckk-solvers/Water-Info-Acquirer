# jma_rainfall_pipeline/fetcher/fetcher.py
from datetime import date, datetime, timedelta, time
from requests.exceptions import RequestException

from jma_rainfall_pipeline.logger.app_logger import get_logger
from jma_rainfall_pipeline.utils.http_client import throttled_get


class Fetcher:
    """
    Fetcher クラス: JMA（気象庁）の観測所データをHTTPで取得する。
    """

    def __init__(
        self,
        base_url: str,
        interval: timedelta,
        default_station_type: str = "s1",
        timeout: int = 10,
    ):
        """
        :param base_url: 気象庁データサイトのベースURL
        :param interval: 取得対象の観測間隔（例: 1日/1時間/10分）
        :param default_station_type: デフォルト観測方式（'s1' または 'a1'）
        :param timeout: HTTPリクエストのタイムアウト秒
        """
        self.base_url = base_url.rstrip("/")
        self.interval = interval
        self.default_station_type = default_station_type
        self.timeout = timeout
        self.logger = get_logger(__name__)

    def _determine_freq(self) -> str:
        """interval から daily/hourly/10min を判定する。"""
        if self.interval >= timedelta(days=1):
            return "daily"
        if self.interval >= timedelta(hours=1):
            return "hourly"
        return "10min"

    def _normalize_station_type(self, station_type: str | None) -> str:
        """観測種別文字列をJMAの命名（a1/s1）に合わせる。"""
        if not station_type:
            return "s1"
        normalized = station_type.strip().lower()
        if not normalized:
            return "s1"
        if normalized.endswith("1"):
            return normalized
        return f"{normalized}1"

    def _build_url(
        self,
        prec_no: str,
        block_no: str,
        target_date: date,
        station_type: str | None = None,
        freq: str | None = None,
    ) -> str:
        """観測対象日・地点から取得URLを生成する。"""
        freq = freq or self._determine_freq()
        st_type = self._normalize_station_type(station_type or self.default_station_type)
        return (
            f"{self.base_url}/stats/etrn/view/{freq}_{st_type}.php"
            f"?prec_no={prec_no}&block_no={block_no}"
            f"&year={target_date.year}&month={target_date.month}&day={target_date.day}&view=1"
        )

    def _build_request_headers(self, freq: str, station_type: str) -> dict[str, str]:
        """観測種別に応じた Referer ヘッダーを組み立てる。"""
        referer_url = f"{self.base_url}/stats/etrn/view/{freq}_{station_type}.php"
        return {"Referer": referer_url}

    def _request_html(self, url: str, freq: str, station_type: str) -> str:
        """HTTPリクエストを投げてHTML文字列を返す。"""
        headers = self._build_request_headers(freq, station_type)
        try:
            resp = throttled_get(url, headers=headers, timeout=self.timeout)
        except RequestException as exc:
            raise RequestException(f"{url} からデータ取得に失敗しました") from exc
        return resp.text

    def fetch(
        self,
        prec_no: str,
        block_no: str,
        target_date: datetime | date,
        station_type: str | None = None,
    ) -> str:
        """
        指定した観測所・日時のHTMLを取得する。
        :param prec_no: 府県番号
        :param block_no: 観測所番号
        :param target_date: 取得対象の日付または日時
        :param station_type: 観測方式（'s1' もしくは 'a1'）
        :return: HTML文字列
        """
        # datetime の場合は date に揃える
        if isinstance(target_date, datetime):
            target_day = target_date.date()
        else:
            target_day = target_date
        freq = self._determine_freq()
        st_type = self._normalize_station_type(station_type or self.default_station_type)
        url = self._build_url(prec_no, block_no, target_day, st_type, freq=freq)
        self.logger.debug("取得URL: %s", url)
        return self._request_html(url, freq, st_type)

    def _get_month_range(self, start: date, end: date) -> list[tuple[date, date]]:
        """
        期間を1か月単位に分割して返す。
        :return: [(month_start, month_end), ...]
        """
        ranges = []
        current = start.replace(day=1)
        while current <= end:
            # 月初
            month_start = current
            # 月末
            if current.month == 12:
                next_month = current.replace(year=current.year + 1, month=1, day=1)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
            month_end = next_month - timedelta(days=1)

            # 取得対象範囲に合わせて切り詰め
            month_start = max(month_start, start)
            month_end = min(month_end, end)

            if month_start <= month_end:
                ranges.append((month_start, month_end))

            current = next_month
        return ranges

    def schedule_fetch(
        self,
        stations: list[tuple[str, str, str | None]],
        start: datetime | date,
        end: datetime | date,
    ):
        """
        観測所リストと期間を与えて順次HTMLを取得するジェネレーター。
        :param stations: (prec_no, block_no, station_type) の配列
        :param start: 取得期間の開始日（datetime でも可）
        :param end: 取得期間の終了日（datetime でも可）
        :yield: ((prec_no, block_no), datetime, HTML)
        """
        # start/end が date の場合は datetime に揃える
        if isinstance(start, date) and not isinstance(start, datetime):
            start = datetime.combine(start, time.min)
        if isinstance(end, date) and not isinstance(end, datetime):
            end = datetime.combine(end, time.max)

        freq = self._determine_freq()

        if freq == "daily":
            # 日別データは月単位でまとめて取得
            month_ranges = self._get_month_range(start.date(), end.date())
            for month_start, month_end in month_ranges:
                # 月初日でまとめて取得（レスポンスは月全体分）
                fetch_date = month_start.replace(day=1)
                for prec_no, block_no, st_type in stations:
                    normalized_type = self._normalize_station_type(st_type or self.default_station_type)
                    url = self._build_url(prec_no, block_no, fetch_date, normalized_type, freq=freq)
                    html = self._request_html(url, freq, normalized_type)
                    # 月初日の datetime をキーとして返す
                    yield (prec_no, block_no), datetime.combine(month_start, time.min), html, url
        else:
            # hourly/10min は日単位で取得
            current_date = start.date()
            end_date = end.date()
            while current_date <= end_date:
                for prec_no, block_no, st_type in stations:
                    normalized_type = self._normalize_station_type(st_type or self.default_station_type)
                    url = self._build_url(prec_no, block_no, current_date, normalized_type, freq=freq)
                    html = self._request_html(url, freq, normalized_type)
                    yield (prec_no, block_no), datetime.combine(current_date, time.min), html, url
                current_date += timedelta(days=1)


# 参考URL
# https://www.data.jma.go.jp/stats/etrn/view/10min_s1.php?prec_no=11&block_no=47401&year=2025&month=7&day=1&view=
# https://www.data.jma.go.jp/stats/etrn/view/10min_s1.php?prec_no=11&block_no=0002&year=2025&month=7&day=1&view=1
# https://www.data.jma.go.jp/stats/etrn/view/10min_s1.php?prec_no=11&block_no=47401&year=2025&month=7&day=1&view=1
