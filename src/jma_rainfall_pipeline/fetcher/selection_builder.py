# selection_builder.py

from typing import List, Dict, Optional
from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_station_codes


def build_station_list(
    pref_codes: List[str],
    station_codes: Optional[List[str]] = None
) -> List[Dict[str, str]]:
    """
    指定した都道府県コードのみ fetch_station_codes を実行し、
    フィルタ済み station_codes があれば絞り込んで返す。

    Args:
      pref_codes:    都道府県コードのリスト (例: ["11", "13"])
      station_codes: GUI で選択された観測所コードのリスト (例: ["47401", "0002"]),
                     None の場合は全観測所を返す。

    Returns:
      List of {
        "prec_no":    str,
        "block_no":   str,
        "station":    str,
        "obs_method": str
      }
    """
    result: List[Dict[str, str]] = []
    for prec in pref_codes:
        # 指定県の全観測所を取得
        stations = fetch_station_codes(prec)
        for rec in stations:
            block = rec.get("block_no")
            station = rec.get("station", "")
            obs_method = rec.get("obs_method", "")
            # station_codes が指定されていない or 含まれているものだけを追加
            if station_codes is None or block in station_codes:
                result.append({
                    "prec_no":    prec,
                    "block_no":   block,
                    "station":    station,
                    "obs_method": obs_method
                })
    return result


if __name__ == "__main__":
    # テスト実行例
    import pprint
    prefs = ["11", "13"]
    codes = ["47401", "47402"]
    stations = build_station_list(prefs, station_codes=codes)
    pprint.pprint(stations)
