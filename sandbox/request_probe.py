import sys
import requests


def main() -> None:
    """
    単純に指定 URL を requests で叩いてレスポンス内容を表示する最小スクリプト。
    ブラウザっぽいヘッダ(User-Agent など)を付与した版。
    """
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.request_probe <URL>")
        sys.exit(1)

    url = sys.argv[1]

    # ブラウザっぽいヘッダ
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/129.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        "Connection": "close",
        # 必要なら:
        # "Referer": "http://www1.river.go.jp/",
        # "Upgrade-Insecure-Requests": "1",
    }

    print(f"[request] GET {url}")
    print(f"[request] headers={headers}")

    resp = requests.get(url, headers=headers, timeout=30)
    print(f"[response] status={resp.status_code}")
    print(f"[response] content-type={resp.headers.get('Content-Type')}")

    # エンコーディングが未設定なら utf-8 とみなす
    resp.encoding = resp.encoding or "utf-8"

    print("[response] body head ----")
    print(resp.text[:500])
    print("---- end ----")


if __name__ == "__main__":
    main()
