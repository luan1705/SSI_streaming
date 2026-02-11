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
    "asset,indices"
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
asset      = Table("asset", md, schema="details", autoload_with=engine)
indices_tbl = Table("vietnam", md, schema="indices", autoload_with=engine)

SQL_UPDATE_FOREIGN = text("""
    UPDATE details.asset
    SET
      "foreignBuyVol"  = COALESCE(:foreignBuyVol,  "foreignBuyVol"),
      "foreignSellVol" = COALESCE(:foreignSellVol, "foreignSellVol"),
      "foreignRoom"    = COALESCE(:foreignRoom,    "foreignRoom"),
      "foreignBuyVal"  = COALESCE(:foreignBuyVal,  "foreignBuyVal"),
      "foreignSellVal" = COALESCE(:foreignSellVal, "foreignSellVal")
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
def content_to_row_asset_table(content: dict) -> dict:

    return {
        "symbol": content.get("symbol"),
        "ceiling":  content.get("ceiling"),
        "floor":    content.get("floor"),
        "refPrice": content.get("refPrice"),

        "buyPrice1": content.get("buyPrice1"), "buyVol1": content.get("buyVol1"),
        "buyPrice2": content.get("buyPrice2"), "buyVol2": content.get("buyVol2"),
        "buyPrice3": content.get("buyPrice3"), "buyVol3": content.get("buyVol3"),

        "matchPrice": content.get("matchPrice"),
        "matchVol": content.get("matchVol"),
        "matchChange": content.get("matchChange"),
        "matchRatioChange": content.get("matchRatioChange"),

        "sellPrice1": content.get("sellPrice1"), "sellVol1": content.get("sellVol1"),
        "sellPrice2": content.get("sellPrice2"), "sellVol2": content.get("sellVol2"),
        "sellPrice3": content.get("sellPrice3"), "sellVol3": content.get("sellVol3"),

        "totalVol": content.get("totalVol"),
        "totalVal": content.get("totalVal"),

        "high": content.get("high"),
        "low":  content.get("low"),
        "open": content.get("open"),
        "close": content.get("close"),
    }

def content_to_row_indices(content: dict) -> dict:

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

        "advancers": content.get("advancers"),
        "noChanges": content.get("noChanges"),
        "decliners": content.get("decliners"),
        "advancersVal": content.get("advancersVal"),
        "noChangesVal": content.get("noChangesVal"),
        "declinersVal": content.get("declinersVal"),

        "ceiling": content.get("ceiling"),
        "floor": content.get("floor")
    }

# ======================
# Batch helpers
# ======================
def upsert_many(conn, tbl: Table, rows: list[dict], pk: str = "symbol"):
    if not rows:
        return

    # chỉ update các cột xuất hiện trong batch rows
    cols_in_rows = set().union(*(r.keys() for r in rows))
    cols_in_rows.discard(pk)

    stmt = pg_insert(tbl).values(rows)
    update_dict = {c: getattr(stmt.excluded, c) for c in cols_in_rows if c in tbl.c}

    if update_dict:
        stmt = stmt.on_conflict_do_update(index_elements=[pk], set_=update_dict)
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=[pk])

    conn.execute(stmt)


# ======================
# Main (coalesce + flush)
# ======================
def main():
    pubsub = r.pubsub()
    pubsub.psubscribe(*[p.strip() for p in PATTERNS if p.strip()])
    logging.info("DB_WRITER(BATCH) listening patterns=%s ...", PATTERNS)

    # coalesce buffers: chỉ giữ latest theo symbol
    asset_buf: dict[str, dict]  = {}
    indices_buf: dict[str, dict] = {}
    foreign_buf: dict[str, dict] = {}
    price_buf: dict[str, float]  = {}

    last_flush = time.time()
    last_log = time.time()
    processed = 0

    def flush():
        nonlocal last_flush, asset_buf, indices_buf, foreign_buf, price_buf

        if not (asset_buf or indices_buf or foreign_buf or price_buf):
            last_flush = time.time()
            return

        # snapshot rồi clear nhanh để không chặn ingest
        e_rows = list(asset_buf.values());  asset_buf.clear()
        i_rows = list(indices_buf.values()); indices_buf.clear()
        f_rows = list(foreign_buf.values()); foreign_buf.clear()
        p_rows = [{"symbol": k, "value": v} for k, v in price_buf.items()]; price_buf.clear()

        t0 = time.time()
        try:
            with engine.begin() as conn:
                # upsert asset / indices theo lô
                upsert_many(conn, asset, e_rows, pk="symbol")
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

        channel = msg.get("channel")
        content = (raw.get("content") if isinstance(raw, dict) and "content" in raw else raw) or {}
        symbol = content.get("symbol")

        # ===== coalesce into buffers =====
        if channel == "asset" and symbol:
            # ====== asset (asset) ======
            if "matchPrice" in content:
                row = content_to_row_asset_table(content)
                asset_buf[symbol] = row

                px = content.get("matchPrice")
                if px is not None:
                    price_buf[symbol] = px

            # ====== FOREIGN (asset) ======
            elif any(k in content for k in ("foreignBuyVol", "foreignSellVol", "foreignRoom", "foreignBuyVal", "foreignSellVal")):
                foreign_buf[symbol] = {
                    "symbol": symbol,
                    "foreignBuyVol":  content.get("foreignBuyVol"),
                    "foreignSellVol": content.get("foreignSellVol"),
                    "foreignRoom":    content.get("foreignRoom"),
                    "foreignBuyVal":  content.get("foreignBuyVal"),
                    "foreignSellVal": content.get("foreignSellVal"),
                }

        elif channel == "indices" and symbol:
            indices_buf[symbol] = content_to_row_indices(content)

        processed += 1

        # tránh buffer phình nếu DB quá chậm
        if (len(asset_buf) + len(indices_buf) + len(foreign_buf) + len(price_buf)) >= MAX_BUFFER_SIZE:
            flush()

        # flush theo thời gian
        if (time.time() - last_flush) * 1000 >= FLUSH_INTERVAL_MS:
            flush()

        # log throughput
        if time.time() - last_log >= 5:
            logging.info("processed=%d buffers: e=%d i=%d f=%d p=%d",
                         processed, len(asset_buf), len(indices_buf), len(foreign_buf), len(price_buf))
            last_log = time.time()

if __name__ == "__main__":
    main()
