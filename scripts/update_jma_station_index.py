# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pdfplumber==0.11.4",
# ]
# ///

import json
import shutil
import sys
from pathlib import Path

# srcディレクトリをパスに追加して river_meta モジュールをインポート可能にする
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from river_meta.amedas.extract import extract_amedas_table_rows, normalize_pref_name

def main() -> None:
    root_dir = Path(__file__).parent.parent
    pdf_path = root_dir / "data" / "source" / "amedas" / "ame_master.pdf"
    json_path = root_dir / "src" / "river_meta" / "resources" / "jma_station_index.json"
    backup_path = json_path.with_suffix(".json.bak")
    
    if not pdf_path.exists():
        print(f"Error: PDFが見つかりません: {pdf_path}")
        return
        
    if not json_path.exists():
        print(f"Error: JSONが見つかりません: {json_path}")
        return

    print("--- 1. PDFから観測所データを抽出 ---")
    rows, stats = extract_amedas_table_rows(in_pdf=str(pdf_path))
    print(f"PDFパース完了: {len(rows)}件取得 (全{stats.total_rows}行中 / {stats.skipped_rows}行スキップ)")

    # 都道府県名＋観測所名の複合キーと、観測所名単体の辞書を高度に作成
    # 1つの名前に複数の地域が紐づく可能性も考慮し、リストで保持
    lookup_name = {}
    
    for r in rows:
        pref = normalize_pref_name(r["prefecture"]).replace("地方", "") # 地方も削る
        name = r["station_name"]
        
        # 観測所名に対する候補リストに追加
        candidates = lookup_name.setdefault(name, [])
        candidates.append({"pref": pref, "data": r})

    print("\n--- 2. JSONデータのバックアップ作成 ---")
    print(f"Backup: {backup_path}")
    shutil.copy2(json_path, backup_path)

    print("\n--- 3. JSONとPDFデータとのマージ ---")
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    updated_exact = 0
    updated_fuzzy_pref = 0
    updated_name_only = 0
    not_found_count = 0

    # by_block_no に入っているJSONデータを反復処理して属性を追加
    for block_no, stations in data.get("by_block_no", {}).items():
        for st in stations:
            api_pref = normalize_pref_name(st.get("pref_name", "")).replace("地方", "")
            name = st.get("station_name", "")
            
            match = None
            match_type = ""
            
            candidates = lookup_name.get(name, [])
            
            if len(candidates) > 0:
                # 1. 完全一致 or 部分一致 (北海道特例含む) で探す
                for cand in candidates:
                    pdf_pref = cand["pref"]
                    
                    # 完全一致
                    if api_pref == pdf_pref:
                        match = cand["data"]
                        match_type = "exact"
                        break
                    
                    # 部分一致 (「網走・北見・紋別」と「網走」など)
                    if api_pref in pdf_pref or pdf_pref in api_pref:
                        match = cand["data"]
                        match_type = "fuzzy_pref"
                        break
                        
                    # 北海道特例: APIは「北海道」だが、PDFは「宗谷」「上川」などになっている
                    if api_pref == "北海道" and pdf_pref not in ("青森", "秋田", "岩手"): # 東北以外なら大体北海道のサブ管区
                        # ※本来は厳密なリストが良いですが簡易的に
                        match = cand["data"]
                        match_type = "fuzzy_pref"
                        break
                
                # 2. それでも見つからず、かつ候補が1件だけなら観測所名のみで採用
                if not match and len(candidates) == 1:
                    match = candidates[0]["data"]
                    match_type = "name_only"
                elif not match and len(candidates) > 1:
                    print(f"[SKIP] 名前が重複し地域も一致しないためスキップ: {api_pref} - {name} (候補: {[c['pref'] for c in candidates]})")
                
            if match:
                st["station_id"] = match["station_id"]
                st["latitude"] = match.get("latitude", "")
                st["longitude"] = match.get("longitude", "")
                st["elevation_m"] = match.get("elevation_m", "")
                
                if match_type == "exact":
                    updated_exact += 1
                elif match_type == "fuzzy_pref":
                    updated_fuzzy_pref += 1
                else:
                    updated_name_only += 1
            else:
                if len(candidates) == 0:
                    print(f"[WARN] 見つかりません: {api_pref} - {name}")
                st["station_id"] = ""
                st["latitude"] = ""
                st["longitude"] = ""
                st["elevation_m"] = ""
                not_found_count += 1

    print(f"\nマージ結果:")
    print(f"  - 完全一致(都道府県+観測所名) : {updated_exact}件")
    print(f"  - 観測所名のみで一致          : {updated_name_only}件")
    print(f"  - 合計更新件数                : {updated_exact + updated_name_only}件")
    if not_found_count > 0:
        print(f"注意: PDF内に見つからなかった観測所が {not_found_count}件 あります。(station_id は空文字になります)")

    print(f"\n--- 4. 更新したJSONを保存 ---")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        
    print(f"完了: {json_path} を上書きしました！")

if __name__ == "__main__":
    main()
