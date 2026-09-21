# update_exchange_lists.py
import re
import requests
import pandas as pd
from pathlib import Path
from typing import Optional, Iterable, List, Dict

# ====== CẤU HÌNH ======
TARGET_FILE = Path("indices_map.py")     # file .py đích
SORT_DEDUP = True                 # loại trùng + sort để ổn định

# Mỗi nguồn -> 1 entry trong dict indices_map
SOURCES = [
    {
        "key": "FUTUREINDEX",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/fu?hasVN30=true&hasVN100=true",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
    },
    {
        "key": "FUTURETPCP",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/gb?hasVN30=true&hasVN100=true",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
    },
]

HEADERS = {
    'authority': 'iboard-query.ssi.com.vn',
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate',#, br, zstd',
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

# ====== HÀM TIỆN ÍCH ======
def format_dict_entry(key: str, items: Iterable[str], indent_key: int = 4, indent_item: int = 8, per_line: int = 10, trailing_comma: bool = True) -> str:
    items = list(items)
    ik = " " * indent_key
    ii = " " * indent_item
    lines = [f"{ik}'{key}':["]
    if items:
        for i in range(0, len(items), per_line):
            block = items[i:i+per_line]
            lines.append(ii + ",\t".join(f"'{s}'" for s in block) + ",")
    lines.append(f"{ik}]{',' if trailing_comma else ''}")
    return "\n".join(lines)

def upsert_block(file_path: Path, tag: str, payload: str):
    start = f"# <{tag} START>"
    end   = f"# <{tag} END>"
    block = f"{start}\n{payload}{end}\n"

    text = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
    pattern = re.compile(rf"{re.escape(start)}.*?{re.escape(end)}\n?", re.DOTALL)

    if pattern.search(text):
        new_text = pattern.sub(block, text)
    else:
        if text and not text.endswith("\n"):
            text += "\n"
        new_text = text + ("\n" if text else "") + block

    tmp = file_path.with_suffix(file_path.suffix + ".tmp")
    tmp.write_text(new_text, encoding="utf-8")
    tmp.replace(file_path)

def fetch_symbols(url: str, headers: dict, data_path: List[str], symbol_field: str,
                  symbol_len_max: Optional[int]) -> List[str]:
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    raw = r.json()

    node = raw
    for k in data_path:
        node = node[k]

    df = pd.json_normalize(node)
    series = df[symbol_field].dropna().astype(str).str.upper()
    if symbol_len_max is not None:
        series = series[series.str.len() <= symbol_len_max]

    symbols = series.tolist()
    if SORT_DEDUP:
        symbols = sorted(set(symbols))
    return symbols

# ====== MAIN ======
def main():
    entries: List[str] = []
    for i, src in enumerate(SOURCES):
        key = src["key"]
        url = src["url"]
        data_path = src["data_path"]
        symbol_field = src.get("symbol_field", "stockSymbol")
        symbol_len_max = src.get("symbol_len_max", None)
        per_line = int(src.get("per_line", 10))

        symbols = fetch_symbols(url, HEADERS, data_path, symbol_field, symbol_len_max)
        # entry nào KHÔNG phải là cuối cùng thì có dấu phẩy; cuối cùng thì không
        trailing = (i != len(SOURCES) - 1)
        entries.append(format_dict_entry(key, symbols, indent_key=4, indent_item=8, per_line=per_line, trailing_comma=trailing))
        print(f"✅ {key}: {len(symbols)} symbols")

    body = "\n".join(entries) + "\n"
    upsert_block(TARGET_FILE, "INDICES_DERIVATIVES", body)
    print(f"➡️  Updated BODY in {TARGET_FILE.resolve()}")

if __name__ == "__main__":
    main()
