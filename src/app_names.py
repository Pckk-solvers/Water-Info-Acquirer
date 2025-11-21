"""アプリ/モジュール名称の一元管理。"""

APP = {
    "jp": "水文情報ツール",
    "en": "Water Info Suite",
}

MODULES = {
    "water_info": {
        "jp": "水文データ取得",
        "en": "Water Info",
    },
    "jma": {
        "jp": "JMA 雨量パイプライン",
        "en": "JMA Rainfall Pipeline",
    },
}


def get_app_title(lang: str = "jp") -> str:
    return APP.get(lang, APP["jp"])


def get_module_title(key: str, lang: str = "jp") -> str:
    return MODULES.get(key, {}).get(lang, key)
