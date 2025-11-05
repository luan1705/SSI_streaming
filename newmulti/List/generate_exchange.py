# update_exchange_lists.py
import re
import requests
import pandas as pd
from pathlib import Path
from typing import Optional, Iterable, List

# ====== CẤU HÌNH CHUNG ======
TARGET_FILE = Path("exchange.py")   # file .py đích
SORT_DEDUP = True                   # True: loại trùng + sort để ổn định giữa các lần chạy

# Mỗi nguồn: 
# - Nếu single_var=False => chia thành PREFIX1, PREFIX2, ... (dùng chunk_size)
# - Nếu single_var=True  => chỉ 1 biến duy nhất: PREFIX = [...]
SOURCES = [
    # Chia nhóm
    {
        "prefix": "HOSE",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/hose?boardId=MAIN",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": 3,
        "chunk_size": 40,
        "per_line": 10,
        "single_var": False,   # <-- CHIA
    },
    {
        "prefix": "HNX",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/hnx?boardId=MAIN",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": 3,
        "chunk_size": 40,
        "per_line": 10,
        "single_var": False,   # <-- CHIA
    },
    {
        "prefix": "UPCOM",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/upcom?boardId=MAIN",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": 3,   # nếu muốn giữ cả 4 ký tự thì đặt None
        "chunk_size": 260,
        "per_line": 10,
        "single_var": False,   # <-- CHIA
    },

    # 1 list duy nhất
    {
        "prefix": "DERIVATIVES",
        "url": "https://iboard-query.ssi.com.vn/stock/exchange/fu?hasVN30=true&hasVN100=true",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
        "single_var": True,    # <-- 1 LIST
    },
    {
        "prefix": "CW",
        "url": "https://iboard-query.ssi.com.vn/stock/cw/hose",
        "data_path": ["data", "coveredWarrantData"],   # <-- đường dẫn riêng của CW
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
        "single_var": True,    # <-- 1 LIST (CW = [...])
    },
    {
        "prefix": "HNXBOND",
        "url": "https://iboard-query.ssi.com.vn/stock/type/b/hnxbond",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
        "single_var": True,    # <-- 1 LIST
    },
    {
        "prefix": "ETFHOSE",
        "url": "https://iboard-query.ssi.com.vn/stock/type/e/hose",
        "data_path": ["data"],
        "symbol_field": "stockSymbol",
        "symbol_len_max": None,
        "per_line": 10,
        "single_var": True,    # <-- 1 LIST
    },
]

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

# ====== HÀM TIỆN ÍCH ======
def chunked(lst: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        raise ValueError("chunk size must be > 0")
    return [lst[i:i+n] for i in range(0, len(lst), n)]

def format_py_list(name: str, items: Iterable[str], indent: int = 4, per_line: int = 10) -> str:
    sp = " " * indent
    items = list(items)
    out = [f"{name} = ["]
    if items:
        for i in range(0, len(items), per_line):
            block = items[i:i+per_line]
            quoted = ",\t".join(f'"{s}"' for s in block)
            out.append(f"{sp}{quoted},")
    out.append("]")
    return "\n".join(out)

def render_numbered_prefix_block(prefix: str, symbols: List[str], chunk_size: int, per_line: int) -> str:
    parts = []
    for i, items in enumerate(chunked(symbols, chunk_size), start=1):
        var_name = f"{prefix}{i}"   # VD: HOSE1, HNX2, UPCOM3...
        parts.append(format_py_list(var_name, items, indent=4, per_line=per_line))
        parts.append("")            # dòng trống giữa các biến
    return "\n".join(parts).rstrip() + "\n"

def render_single_var_block(var_name: str, symbols: List[str], per_line: int) -> str:
    return format_py_list(var_name, symbols, indent=4, per_line=per_line) + "\n"

def upsert_block(file_path: Path, tag: str, payload: str):
    """
    Insert/replace payload vào giữa:
      # <TAG START>
      ...payload...
      # <TAG END>
    Nếu chưa có -> append cuối file (thêm marker và nội dung).
    Ghi 'atomic' qua .tmp để tránh hỏng file nếu sự cố giữa chừng.
    """
    start = f"# <{tag} START>"
    end   = f"# <{tag} END>"
    block = f"{start}\n{payload}{end}\n"

    text = file_path.read_text(encoding="utf-8") if file_path.exists() else ""
    pattern = re.compile(rf"{re.escape(start)}.*?{re.escape(end)}\n?", re.DOTALL)

    if pattern.search(text):
        new_text = pattern.sub(block, text)   # ghi đè block cũ
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

    # Truy cập tới mảng data theo data_path (vd CW: ["data","coveredWarrantData"])
    node = raw
    for k in data_path:
        node = node[k]

    df = pd.json_normalize(node)
    series = df[symbol_field].dropna().astype(str).str.upper()
    if symbol_len_max is not None:
        series = series[series.str.len() <= symbol_len_max]

    symbols = series.tolist()
    if SORT_DEDUP:
        symbols = sorted(set(symbols))  # loại trùng + ổn định thứ tự
    return symbols

# ====== MAIN ======
def main():
    for src in SOURCES:
        prefix = src["prefix"]
        url = src["url"]
        data_path = src["data_path"]
        symbol_field = src.get("symbol_field", "stockSymbol")
        symbol_len_max = src.get("symbol_len_max", None)
        per_line = int(src.get("per_line", 10))
        single_var = bool(src.get("single_var", False))

        symbols = fetch_symbols(url, HEADERS, data_path, symbol_field, symbol_len_max)

        if single_var:
            # 1 biến duy nhất: PREFIX = [...]
            payload = render_single_var_block(prefix, symbols, per_line)
        else:
            # Chia nhóm: PREFIX1, PREFIX2, ...
            chunk_size = int(src.get("chunk_size", 40))
            payload = render_numbered_prefix_block(prefix, symbols, chunk_size, per_line)

        upsert_block(TARGET_FILE, prefix, payload)
        print(f"✅ Updated {TARGET_FILE.resolve()} | {prefix}: symbols={len(symbols)}")

if __name__ == "__main__":
    main()
