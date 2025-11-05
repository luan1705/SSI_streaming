# generate_indice.py
import re
import time
import requests
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict

TARGET_FILE = Path("indices_map.py")
SORT_DEDUP = True

HEADERS = {
    'authority': 'iboard-query.ssi.com.vn',
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://iboard.ssi.com.vn',
    'referer': 'https://iboard.ssi.com.vn/',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'
}

# Lưu tên bạn muốn thấy trong file output
INDICE_NAMES = [
    "VNINDEX","HNXINDEX","UPCOMINDEX","VN100","VN30",
    "VNALLSHARE","VNCOND","VNCONS","VNDIAMOND","VNENE",
    "VNFIN","VNFINLEAD","VNFINSELECT","VNHEAL","VNIND",
    "HNX30","VNIT","VNMAT","VNMIDCAP","VNREAL",
    "VNSI","VNSMALLCAP","VNUTI","VNX50","VNXALLSHARE"
]

# Map tên LƯU → code API gọi
INDEX_CODE_MAP: Dict[str, str] = {
    "VNINDEX": "VNINDEX",
    "HNXINDEX": "HNXIndex",
    "UPCOMINDEX": "HNXUpcomIndex",  # <-- API code, nhưng key lưu là 'UPCOMINDEX'
    "VN100": "VN100",
    "VN30": "VN30",

    "VNALLSHARE": "VNALL",
    "VNMIDCAP": "VNMID",
    "VNSMALLCAP": "VNSML",
    "VNXALLSHARE": "VNXALL",

    "VNCOND": "VNCOND",
    "VNCONS": "VNCONS",
    "VNDIAMOND": "VNDIAMOND",
    "VNENE": "VNENE",
    "VNFIN": "VNFIN",
    "VNFINLEAD": "VNFINLEAD",
    "VNFINSELECT": "VNFINSELECT",
    "VNHEAL": "VNHEAL",
    "VNIND": "VNIND",
    "HNX30": "HNX30",
    "VNIT": "VNIT",
    "VNMAT": "VNMAT",
    "VNREAL": "VNREAL",
    "VNSI": "VNSI",
    "VNUTI": "VNUTI",
    "VNX50": "VNX50",
}

BASE_URL = "https://iboard-query.ssi.com.vn/stock/group/{}"

def infer_len_max(index_name: str) -> Optional[int]:
    # UPCOM => không giới hạn (có thể 4 ký tự); HNX/VN => 3 ký tự
    up = index_name.upper()
    if "UPCOM" in up:
        return None
    if "HNX" in up:
        return 3
    return 3

def try_json(r: requests.Response):
    try:
        return r.json()
    except Exception:
        head = (r.text or "")[:160].replace("\n", " ").replace("\r", " ")
        raise RuntimeError(f"Non-JSON response (status={r.status_code}, head='{head}').")

def fetch_index_symbols(index_name: str, len_max: Optional[int], retries: int = 2, backoff_sec: float = 0.7) -> List[str]:
    code = INDEX_CODE_MAP.get(index_name, index_name)
    url = BASE_URL.format(code)
    last_err = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code != 200:
                raise RuntimeError(f"HTTP {r.status_code}")
            raw = try_json(r)
            if "data" not in raw:
                raise RuntimeError(f"Missing 'data' key. Keys: {list(raw.keys())}")
            df = pd.json_normalize(raw["data"])
            if "stockSymbol" not in df.columns:
                raise RuntimeError(f"'stockSymbol' column not in response. Columns: {list(df.columns)}")
            series = df["stockSymbol"].dropna().astype(str).str.upper()
            if len_max is not None:
                series = series[series.str.len() <= len_max]
            symbols = series.tolist()
            if SORT_DEDUP:
                symbols = sorted(set(symbols))
            return symbols
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_sec * (attempt + 1))
                continue
            raise

def format_py_list_multiline(items: List[str], indent: int = 8, per_line: int = 16) -> str:
    sp = " " * indent
    out = ["["]
    for i in range(0, len(items), per_line):
        block = items[i:i+per_line]
        out.append(f"{sp}" + ",".join(f"'{s}'" for s in block) + ",")
    out.append("]")
    return "\n".join(out)

def render_indices_block(indices_map: Dict[str, List[str]], close_brace: bool = True) -> str:
    parts = ["indices_map = {"]
    # giữ thứ tự bạn đã khai báo
    for name in INDICE_NAMES:
        body = format_py_list_multiline(indices_map.get(name, []), indent=8, per_line=16)
        parts.append(f"    '{name}': {body},")
    if close_brace:
        parts.append("}")
    return "\n".join(parts) + "\n"

def upsert_block(file_path: Path, tag: str, payload: str):
    start = f"# <{tag} START>"
    end   = f"# <{tag} END>"
    block = f"{start}\n{payload}{end}\n"
    text = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
    pattern = re.compile(rf"{re.escape(start)}.*?{re.escape(end)}\n?", re.DOTALL)
    new_text = pattern.sub(block, text) if pattern.search(text) else (text + ("\n" if text and not text.endswith("\n") else "") + block)
    tmp = file_path.with_suffix(file_path.suffix + ".tmp")
    tmp.write_text(new_text, encoding="utf-8")
    tmp.replace(file_path)

def main():
    indices_map: Dict[str, List[str]] = {}
    for name in INDICE_NAMES:
        ln = infer_len_max(name)
        try:
            syms = fetch_index_symbols(name, ln)
            indices_map[name] = syms
            print(f"{name}: {len(syms)} symbols (len_max={ln})")
        except Exception as e:
            print(f"[WARN] Failed to fetch {name} (code={INDEX_CODE_MAP.get(name, name)}): {e}")
            indices_map[name] = []

    payload = render_indices_block(indices_map, close_brace=False)  # <-- KHÔNG đóng }
    upsert_block(TARGET_FILE, "INDICES", payload)
    print(f"✅ Updated {TARGET_FILE.resolve()} with block: INDICES")

if __name__ == "__main__":
    main()
