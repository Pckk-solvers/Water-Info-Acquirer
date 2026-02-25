from __future__ import annotations

import re
from typing import Optional


def extract_total_count(html: str) -> Optional[int]:
    match = re.search(r"全部で\s*([0-9]+)\s*件", html)
    return int(match.group(1)) if match else None


def extract_station_ids(html: str) -> set[str]:
    ids = set(re.findall(r"SiteDetail1\('([0-9]{8,20})'\)", html))
    ids |= set(re.findall(r"SiteInfo\.exe\?ID=([0-9]{8,20})", html))
    return ids


def extract_prefectures(html: str) -> list[tuple[str, str]]:
    prefs = re.findall(
        r'<OPTION\s+VALUE="([0-9]{4})"[^>]*>\s*([^<]+)\s*</OPTION>',
        html,
        flags=re.IGNORECASE,
    )
    return [(code, name.strip()) for code, name in prefs if code != "-1"]


def extract_komoku_options(html: str) -> list[tuple[str, str]]:
    items = re.findall(
        r'<OPTION\s+VALUE="(-?[0-9]{1,2})"[^>]*>\s*([^<]+)\s*</OPTION>',
        html,
        flags=re.IGNORECASE,
    )
    normalized: list[tuple[str, str]] = []
    for code, name in items:
        text = name.strip()
        if "観測項目" in text:
            continue
        normalized.append((code.strip(), text))
    return normalized


def normalize_pref_token(value: str) -> str:
    text = value.replace("\u3000", " ").strip()
    text = re.sub(r"\s+", "", text)
    return text


def normalize_item_token(value: str) -> str:
    text = value.replace("\u3000", " ").strip()
    text = re.sub(r"\s+", "", text)
    text = text.replace("・", "").replace("･", "").replace("/", "")
    return text


def build_prefecture_maps(
    pref_options: list[tuple[str, str]],
) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    code_to_name: dict[str, str] = {}
    alias_to_pref: dict[str, tuple[str, str]] = {}

    for code, name in pref_options:
        code_to_name[code] = name

        aliases: set[str] = {normalize_pref_token(name)}
        if name.endswith(("都", "府", "県")) and len(name) >= 2:
            aliases.add(normalize_pref_token(name[:-1]))
        if name == "北海道":
            aliases.add("北海道")

        for alias in aliases:
            alias_to_pref[alias] = (code, name)

    return code_to_name, alias_to_pref


def build_komoku_maps(
    komoku_options: list[tuple[str, str]],
) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
    code_to_name: dict[str, str] = {}
    alias_to_item: dict[str, tuple[str, str]] = {}
    for code, name in komoku_options:
        code_to_name[code] = name
        aliases = {
            normalize_item_token(name),
            normalize_item_token(name.replace("・", "")),
        }
        if code.startswith("0") and len(code) == 2:
            aliases.add(code[1:])  # 01 -> 1
        aliases.add(code)
        for alias in aliases:
            if alias:
                alias_to_item[alias] = (code, name)
    return code_to_name, alias_to_item


def parse_csv_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def filter_pref_options_by_codes(
    pref_options: list[tuple[str, str]],
    codes_csv: str,
) -> list[tuple[str, str]]:
    if not codes_csv.strip():
        return pref_options
    selected_codes = set(parse_csv_values(codes_csv))
    return [(code, name) for code, name in pref_options if code in selected_codes]


def resolve_prefecture_targets_by_names(
    *,
    pref_options: list[tuple[str, str]],
    alias_to_pref: dict[str, tuple[str, str]],
    code_to_name: dict[str, str],
    pref_names: list[str],
) -> tuple[list[tuple[str, str]], list[str]]:
    resolved: list[tuple[str, str]] = []
    errors: list[str] = []
    seen_codes: set[str] = set()

    for raw in pref_names:
        token = normalize_pref_token(raw)
        if not token:
            continue
        if token in {"all", "ALL", "全国"}:
            for code, name in pref_options:
                if code in seen_codes:
                    continue
                seen_codes.add(code)
                resolved.append((code, name))
            continue

        if re.fullmatch(r"\d{4}", token):
            name = code_to_name.get(token)
            if not name:
                errors.append(raw)
                continue
            if token not in seen_codes:
                seen_codes.add(token)
                resolved.append((token, name))
            continue

        hit = alias_to_pref.get(token)
        if not hit:
            errors.append(raw)
            continue
        code, name = hit
        if code not in seen_codes:
            seen_codes.add(code)
            resolved.append((code, name))

    return resolved, errors


def normalize_komoku_code(code: str) -> str:
    text = code.strip()
    if text == "-1":
        return text
    if re.fullmatch(r"[0-9]{1}", text):
        return f"0{text}"
    return text
