# freeform_parser.py

import re
from typing import List, Dict, Tuple, Any
from jma_rainfall_pipeline.fetcher.selection_builder import build_station_list

class FreeformParserError(Exception):
    """フリーフォームパースエラー用例外"""
    pass


def parse_freeform(
    prefecture_input: str,
    station_input: str,
    prefecture_map: Dict[str, str],
    station_map: Dict[Tuple[str, str], str],
    reverse_station_map: Dict[str, List[Tuple[str, str]]]
) -> List[Dict[str, Any]]:
    """
    フリーフォーム入力（都道府県, 観測所）を解析し、
    build_station_list 経由で station, obs_method を含むリストを返す。

    Args:
      prefecture_input: 県指定文字列, 例: "北海道、11、東京"
      station_input:    観測所指定文字列, 例: "稚内、47401、船泊"
      prefecture_map:    {都道府県名_or_code: code}
      station_map:       {(code, station_name): block_no}
      reverse_station_map: {station_name: [(code, block_no), ...]}

    Returns:
      [
        {"prec_no":code, "block_no":block, "station":name, "obs_method":method},
        ...
      ]

    Raises:
      FreeformParserError: 入力不正や多義性エラー
    """
    # 都道府県コードリスト（「,」と「、」で分割）
    pref_tokens = [t.strip() for t in re.split(r"[,、]", prefecture_input) if t.strip()]
    pref_codes: List[str] = []
    for tok in pref_tokens:
        if re.fullmatch(r"\d{1,2}", tok):
            code = tok.zfill(2)
        else:
            code = prefecture_map.get(tok)
        if not code or code not in prefecture_map.values():
            raise FreeformParserError(f"都道府県 '{tok}' が不正です。")
        if code not in pref_codes:
            pref_codes.append(code)

    # 観測所トークン（「,」と「、」で分割）
    station_tokens = [t.strip() for t in re.split(r"[,、]", station_input) if t.strip()]
    station_codes: List[str] = []
    for tok in station_tokens:
        # 数字のみ = block_no
        if re.fullmatch(r"\d+", tok):
            station_codes.append(tok)
            continue
        # 県名:駅名 or 県名 駅名
        if ':' in tok or ' ' in tok:
            sep = ':' if ':' in tok else ' '
            p_name, s_name = [x.strip() for x in tok.split(sep, 1)]
            code = prefecture_map.get(p_name)
            if not code:
                raise FreeformParserError(f"都道府県 '{p_name}' が不正です。")
            block = station_map.get((code, s_name))
            if not block:
                raise FreeformParserError(f"観測所 '{s_name}' が県{code}に存在しません。")
            station_codes.append(block)
            continue
        # 駅名のみ
        candidates = reverse_station_map.get(tok)
        if not candidates:
            raise FreeformParserError(f"観測所 '{tok}' が認識できません。")
        if len(candidates) > 1:
            raise FreeformParserError(f"観測所 '{tok}' は複数候補: {candidates}")
        station_codes.append(candidates[0][1])

    # build_station_list で obs_method も含めて取得
    return build_station_list(pref_codes, station_codes)
