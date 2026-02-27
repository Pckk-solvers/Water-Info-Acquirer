import requests

def fetch_site_info(station_id: str) -> str:
    url = f"https://www1.river.go.jp/cgi-bin/SiteInfo.exe?ID={station_id}"
    print(f"Fetching: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding
    html = resp.text
    return html

if __name__ == "__main__":
    station_id = "1361160200060" # 和歌山県 観測所
    html = fetch_site_info(station_id)
    
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    
    # テーブルの行を取得
    results = {}
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) >= 2:
            key = tds[0].get_text(strip=True)
            val = tds[1].get_text(strip=True)
            results[key] = val
            
    import json
    with open("site_info.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("Saved to site_info.json")
