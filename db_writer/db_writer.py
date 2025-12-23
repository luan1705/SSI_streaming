import os
import json
import time
import logging
import redis

from sqlalchemy import create_engine, MetaData, Table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ======================
# ENV
# ======================
REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
DB_URL    = os.getenv("DB_URL", "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

PATTERNS = os.getenv(
    "PATTERNS",
    "ebtb*,ebfr*,indices*"
).split(",")

# Flush config
FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "200"))   # 100-500ms tuỳ tải
MAX_BUFFER_SIZE   = int(os.getenv("MAX_BUFFER_SIZE", "5000"))    # chống RAM phình

# ======================
# Redis
# ======================
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=60,
    socket_connect_timeout=5,
    health_check_interval=30,
    max_connections=10,   # tăng chút để pubsub ổn
    timeout=1.0,
)
r = redis.Redis(connection_pool=POOL)

# ======================
# Postgres
# ======================
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "0")),
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "10")),
    pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "1800")),
    connect_args={"application_name": "db_writer_batch"},
    echo=False,
)

md = MetaData()
eboard      = Table("eboard", md, schema="details", autoload_with=engine)
indices_tbl = Table("vietnam", md, schema="indices", autoload_with=engine)

SQL_UPDATE_FOREIGN = text("""
    UPDATE details.eboard
    SET
      "foreignBuyVol"  = :buyVol,
      "foreignSellVol" = :sellVol,
      "foreignRoom"    = :room,
      "foreignBuyVal"  = :buyVal,
      "foreignSellVal" = :sellVal
    WHERE symbol = :symbol
""")

SQL_UPSERT_PRICE_NOW = text("""
    INSERT INTO status."break" (symbol, price_now)
    VALUES (:symbol, :value)
    ON CONFLICT (symbol) DO UPDATE
    SET price_now = EXCLUDED.price_now
""")

# ======================
# Mapping functions (giữ của bạn)
# ======================
def content_to_row_eboard_table(content: dict) -> dict:
    buy   = content.get("buy") or {}
    sell  = content.get("sell") or {}
    match = content.get("match") or {}

    buy_price  = (buy.get("price")  or [])
    buy_vol    = (buy.get("vol")    or [])
    sell_price = (sell.get("price") or [])
    sell_vol   = (sell.get("vol")   or [])

    buy_price  = (buy_price  + [None, None, None])[:3]
    buy_vol    = (buy_vol    + [None, None, None])[:3]
    sell_price = (sell_price + [None, None, None])[:3]
    sell_vol   = (sell_vol   + [None, None, None])[:3]

    return {
        "symbol": content.get("symbol"),
        "ceiling":  content.get("ceiling"),
        "floor":    content.get("floor"),
        "refPrice": content.get("refPrice"),

        "buyPrice1": buy_price[0], "buyVol1": buy_vol[0],
        "buyPrice2": buy_price[1], "buyVol2": buy_vol[1],
        "buyPrice3": buy_price[2], "buyVol3": buy_vol[2],

        "matchPrice": match.get("price"),
        "matchVol": match.get("vol"),
        "matchChange": match.get("change"),
        "matchRatioChange": match.get("ratioChange"),

        "sellPrice1": sell_price[0], "sellVol1": sell_vol[0],
        "sellPrice2": sell_price[1], "sellVol2": sell_vol[1],
        "sellPrice3": sell_price[2], "sellVol3": sell_vol[2],

        "totalVol": content.get("totalVol"),
        "totalVal": content.get("totalVal"),

        "high": content.get("high"),
        "low":  content.get("low"),
        "open": content.get("open"),
        "close": content.get("close"),
    }

def content_to_row_indices(content: dict) -> dict:
    a  = (content.get("advancersDecliners") or [])
    av = (content.get("advancersDeclinersVal") or [])
    cf = (content.get("ceilingFloor") or [])

    a  = (a  + [None, None, None])[:3]
    av = (av + [None, None, None])[:3]
    cf = (cf + [None, None])[:2]

    return {
        "symbol": content.get("symbol"),
        "point": content.get("point"),
        "refPoint": content.get("refPoint"),
        "change": content.get("change"),
        "ratioChange": content.get("ratioChange"),

        "totalMatchVol": content.get("totalMatchVol"),
        "totalMatchVal": content.get("totalMatchVal"),
        "totalDealVol": content.get("totalDealVol"),
        "totalDealVal": content.get("totalDealVal"),
        "totalVol": content.get("totalVol"),
        "totalVal": content.get("totalVal"),

        "advancers": a[0],
        "noChange": a[1],
        "decliners": a[2],

        "advancersVal": av[0],
        "noChangeVal": av[1],
        "declinersVal": av[2],

        "ceiling": cf[0],
        "floor": cf[1],
    }

# ======================
# Batch helpers
# ======================
def upsert_many(conn, tbl: Table, rows: list[dict], pk: str = "symbol"):
    if not rows:
        return
    stmt = pg_insert(tbl).values(rows)
    # update tất cả column trừ pk
    update_dict = {c.name: getattr(stmt.excluded, c.name) for c in tbl.columns if c.name != pk}
    stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=update_dict)
    conn.execute(stmt)

# ======================
# Main (coalesce + flush)
# ======================
def main():
    pubsub = r.pubsub()
    pubsub.psubscribe(*[p.strip() for p in PATTERNS if p.strip()])
    logging.info("DB_WRITER(BATCH) listening patterns=%s ...", PATTERNS)

    # coalesce buffers: chỉ giữ latest theo symbol
    eboard_buf: dict[str, dict]  = {}
    indices_buf: dict[str, dict] = {}
    foreign_buf: dict[str, dict] = {}
    price_buf: dict[str, float]  = {}

    last_flush = time.time()
    last_log = time.time()
    processed = 0

    def flush():
        nonlocal last_flush, eboard_buf, indices_buf, foreign_buf, price_buf

        if not (eboard_buf or indices_buf or foreign_buf or price_buf):
            last_flush = time.time()
            return

        # snapshot rồi clear nhanh để không chặn ingest
        e_rows = list(eboard_buf.values());  eboard_buf.clear()
        i_rows = list(indices_buf.values()); indices_buf.clear()
        f_rows = list(foreign_buf.values()); foreign_buf.clear()
        p_rows = [{"symbol": k, "value": v} for k, v in price_buf.items()]; price_buf.clear()

        t0 = time.time()
        try:
            with engine.begin() as conn:
                # upsert eboard / indices theo lô
                upsert_many(conn, eboard, e_rows, pk="symbol")
                upsert_many(conn, indices_tbl, i_rows, pk="symbol")

                # foreign: executemany update (nhanh)
                if f_rows:
                    conn.execute(SQL_UPDATE_FOREIGN, f_rows)

                # price_now: executemany upsert
                if p_rows:
                    conn.execute(SQL_UPSERT_PRICE_NOW, p_rows)

        except Exception as e:
            logging.error("FLUSH error: %s", e)

        dt = (time.time() - t0) * 1000
        last_flush = time.time()
        if dt > 200:
            logging.warning("flush took %.1fms (e=%d i=%d f=%d p=%d)", dt, len(e_rows), len(i_rows), len(f_rows), len(p_rows))

    while True:
        # blocking hơn get_message loop (ít CPU)
        msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if msg is None:
            # không có message nhưng vẫn flush định kỳ
            if (time.time() - last_flush) * 1000 >= FLUSH_INTERVAL_MS:
                flush()
            continue

        if msg.get("type") != "pmessage":
            continue

        data_str = msg.get("data")
        if not data_str:
            continue

        try:
            raw = json.loads(data_str)
        except Exception:
            continue

        fn = raw.get("function")
        content = raw.get("content") or {}
        symbol = content.get("symbol")

        # ===== coalesce into buffers =====
        if fn == "eboard_table" and symbol:
            row = content_to_row_eboard_table(content)
            eboard_buf[symbol] = row

            match = content.get("match") or {}
            px = match.get("price")
            if px is not None:
                price_buf[symbol] = px

        elif fn == "eboard_foreign" and symbol:
            foreign_buf[symbol] = {
                "symbol": symbol,
                "buyVol":  content.get("buyVol"),
                "sellVol": content.get("sellVol"),
                "room":    content.get("room"),
                "buyVal":  content.get("buyVal"),
                "sellVal": content.get("sellVal"),
            }

        elif fn == "indices" and symbol:
            indices_buf[symbol] = content_to_row_indices(content)

        processed += 1

        # tránh buffer phình nếu DB quá chậm
        if (len(eboard_buf) + len(indices_buf) + len(foreign_buf) + len(price_buf)) >= MAX_BUFFER_SIZE:
            flush()

        # flush theo thời gian
        if (time.time() - last_flush) * 1000 >= FLUSH_INTERVAL_MS:
            flush()

        # log throughput
        if time.time() - last_log >= 5:
            logging.info("processed=%d buffers: e=%d i=%d f=%d p=%d",
                         processed, len(eboard_buf), len(indices_buf), len(foreign_buf), len(price_buf))
            last_log = time.time()

if __name__ == "__main__":
    main()
