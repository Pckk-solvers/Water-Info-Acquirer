import sys

import requests


def main() -> None:
    """
    単純に指定 URL を requests で叩いてレスポンス内容を表示する最小スクリプト。
    """
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.request_probe <URL>")
        sys.exit(1)

    url = sys.argv[1]
    print(f"[request] GET {url}")
    resp = requests.get(url, timeout=30)
    print(f"[response] status={resp.status_code}")
    print(f"[response] content-type={resp.headers.get('Content-Type')}")
    resp.encoding = resp.encoding or "utf-8"
    print("[response] body head ----")
    print(resp.text[:500])
    print("---- end ----")


if __name__ == "__main__":
    main()
