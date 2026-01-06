#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import concurrent.futures
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import logging
import math
import time
import pandas as pd
from sqlalchemy import create_engine

# ================== CẤU HÌNH ==================
PG_URL = "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech"

# Danh sách symbol cần lấy refPoint (dùng thẳng, không map)
SYMBOLS = ["HNXINDEX", "UPCOMINDEX", "HNX30"]
ALLOWED_SYMBOLS = set(SYMBOLS)  # dùng để validate table name
# ==============================================

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("stock_update.log", encoding="utf-8")],
)
log = logging.getLogger("get_refpoint")

# ---------- Engine (giữ sống tới cuối chương trình) ----------
ENGINE = create_engine(
    PG_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

# ================== HÀM ĐỌC DB ==================
def tradingview(symbol: str) -> pd.DataFrame:
    """
    Lấy close mới nhất từ bảng ohlcv."<symbol>_1D"
    Trả về DataFrame có cột 'close' (1 dòng) hoặc DataFrame rỗng nếu lỗi/không có bảng.
    """
    if symbol not in ALLOWED_SYMBOLS:
        log.error("Symbol không nằm trong whitelist: %s", symbol)
        return pd.DataFrame()

    table_name = f'ohlcv."{symbol}_1D"'
    sql = f"""
            SELECT "symbol","close","time"
            FROM {table_name}
            WHERE "time" < date_trunc('day', now())
            ORDER BY "time" DESC
            LIMIT 1
            """

    try:
        with ENGINE.connect() as conn:
            df = pd.read_sql(sql, con=conn)
            if df.empty:
                log.warning("⚠️ Không có dữ liệu cho %s", symbol)
            return df
    except Exception:
        log.exception("Lỗi đọc bảng %s", table_name)
        return pd.DataFrame()

# ================== XỬ LÝ REFPOINT ==================
def _coerce_refpoint(obj):
    """
    Chuẩn hoá kết quả từ tradingview(symbol):
      - Nếu là số: float
      - Nếu là DataFrame có 'close' (+ 'time' nếu có): lấy 'close' mới nhất
      - Nếu là dict có 'close'/'refPoint': lấy ra
      - Ngược lại: None
    """
    if isinstance(obj, (int, float)) and math.isfinite(obj):
        return float(obj)

    if isinstance(obj, pd.DataFrame) and not obj.empty:
        df = obj.copy()
        lowermap = {c.lower(): c for c in df.columns}
        if "close" not in lowermap:
            return None
        if "time" in lowermap:
            df = df.sort_values(lowermap["time"])
        close_col = lowermap["close"]
        val = df.tail(1)[close_col].iloc[0]
        try:
            return float(val)
        except Exception:
            return None

    if isinstance(obj, dict):
        for k in ("refPoint", "refpoint", "close", "Close"):
            if k in obj:
                try:
                    return float(obj[k])
                except Exception:
                    return None
    return None

def get_one(symbol: str):
    """Luôn trả (symbol, value|None, message)."""
    try:
        time.sleep(0.2)  # nhẹ để tránh spam DB
        res = tradingview(symbol)
        val = _coerce_refpoint(res)
        if val is None:
            msg = f"⚠️ Không lấy được refPoint cho {symbol}"
            log.warning(msg)
        else:
            msg = f"✅ {symbol} -> refPoint={val}"
            log.info(msg)
        return symbol, val, msg
    except Exception as e:
        msg = f"❌ Lỗi khi xử lý {symbol}: {e}"
        log.error(msg)
        return symbol, None, msg

def update_all_stocks(symbol_list):
    ref_map, messages = {}, []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(symbol_list))) as ex:
        futs = [ex.submit(get_one, s) for s in symbol_list]
        for f in concurrent.futures.as_completed(futs):
            sym, val, msg = f.result()
            messages.append(msg)
            if val is not None:
                ref_map[sym] = val
    return ref_map, messages

# ================== GHI FILE refpoint.py ==================
def write_refpoint_py(ref_map: dict, out_path: str = "refpoint.py", keep_original: bool = True):
    """
    Ghi file:
    REFPOINT = {
        "HNXINDEX": 267.28,
        "UPCOMINDEX": 111.13,
        "HNX30": 581.23,
    }
    keep_original=True -> giữ nguyên số (không làm tròn), cắt đuôi 0 thừa.
    """
    def fmt_raw(v: float) -> str:
        s = f"{v:.15f}".rstrip("0").rstrip(".")
        return s if s else "0"

    def fmt(v: float) -> str:
        return fmt_raw(v) if keep_original else str(int(round(v)))

    lines = [
        "# AUTO-GENERATED. Do not edit.",
        f'# Generated at: {datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).isoformat(timespec="seconds")}',
        "",
        "REFPOINT = {",
    ]
    for k in sorted(ref_map.keys()):
        lines.append(f'    "{k}": {fmt(ref_map[k])},')
    lines.append("}")
    content = "\n".join(lines) + "\n"

    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(p)

# ================== MAIN ==================
def save_refpoint():
    print("🚀 Bắt đầu lấy refPoint...")
    ref_map, messages = update_all_stocks(SYMBOLS)

    errors = [m for m in messages if m.startswith("❌") or m.startswith("⚠️")]
    if errors:
        log.warning("📛 Chi tiết lỗi:")
        for err in errors:
            log.warning(err)

    # GIỮ NGUYÊN SỐ
    write_refpoint_py(ref_map, out_path="refpoint.py", keep_original=True)

    return ref_map

if __name__ == "__main__":
    try:
        save_refpoint()
        log.info("✅ Hoàn thành tạo file refpoint.py.")
    finally:
        try:
            ENGINE.dispose()
            log.info("🔒 Đã đóng kết nối DB")
        except Exception:
            pass
