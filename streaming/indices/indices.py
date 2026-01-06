import os, json, time, logging, signal, sys, threading, redis
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# ====== SSI stream ======
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# ====== IMPORTS THEO DỰ ÁN ======
from List import confighao as config
#from List.upsert import upsert_mi
from List.indice import INDICES_GROUPS
from indices.refpoint import REFPOINT
# ================================

# ----------------- CHỌN GROUP QUA ENV -----------------
GROUP_KEY = os.getenv("INDICES_GROUP", "").strip()
if not GROUP_KEY:
    raise RuntimeError("Missing INDICES_GROUP. Set INDICES_GROUP=indices1..indices5")

SYMBOL_LIST = INDICES_GROUPS.get(GROUP_KEY)
if not SYMBOL_LIST:
    raise RuntimeError(f"Unknown INDICES_GROUP={GROUP_KEY}. Valid: {list(INDICES_GROUPS.keys())}")

# default channel: indices_1..indices_5 (giống style của mày)
suffix = GROUP_KEY.replace("indices", "").strip()  # "1".."5"
# ------------------------------------------------------

REDIS_URL   = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
CHANNEL     = "indices"
PG_URL      = os.getenv("PG_URL", "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech")

STREAM_CODE = "MI:" + "-".join(SYMBOL_LIST)

# Cache TTL cho query DB (giây)
ADVDEC_CACHE_TTL = int(os.getenv("ADVDEC_CACHE_TTL", "5"))

# Map symbol từ feed -> chuẩn
SYMBOL_MAP = {
    "HNXUpcomIndex": "UPCOMINDEX",
    "HNXIndex":      "HNXINDEX"
}

# -------------- Logging --------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer_mi_indices")

# -------------- Redis --------------
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,
    socket_connect_timeout=2.0,
    health_check_interval=30,
    max_connections=5,
    timeout=1.0,
)
r = redis.Redis(connection_pool=POOL)

def publish(payload: dict):
    try:
        r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        log.warning("Redis publish fail (%s): %s", CHANNEL, e)
        try:
            rr = redis.Redis(connection_pool=POOL)
            rr.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
            log.info("Redis reconnected & published")
        except Exception as e2:
            log.error("Redis retry failed: %s", e2)

# -------------- Postgres --------------
engine = create_engine(
    PG_URL,
    poolclass=QueuePool,
    pool_size=8,
    max_overflow=8,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 3,
    },
    future=True,
)

# Cache adv/dec counts & vals theo index
_adv_cache = {}   # symbol -> {"ts": epoch, "data": (adv, nc, dec)}
_val_cache = {}   # symbol -> {"ts": epoch, "data": (advVal, ncVal, decVal)}
_cefl_cache = {}  # symbol -> {"ts": epoch, "data": (ceil_cnt, floor_cnt)}

def _now_vn():
    return datetime.now(ZoneInfo("Asia/Ho_Chi_Minh"))

def _map_symbol(sym: str) -> str:
    return SYMBOL_MAP.get(sym, sym)

def _get_counts_from_db(index_symbol: str):
    """(adv, nc, dec) từ asset."""
    pattern = f"%|{index_symbol}|%"
    sql = text("""
        SELECT
          SUM(CASE WHEN "matchChange" > 0 THEN 1 ELSE 0 END)::BIGINT     AS adv,
          SUM(CASE WHEN "matchChange" = 0 THEN 1 ELSE 0 END)::BIGINT     AS nc,
          SUM(CASE WHEN "matchChange" < 0 THEN 1 ELSE 0 END)::BIGINT     AS dec
        FROM "details"."asset" left join "info"."stock" using("symbol")
        WHERE "indices" IS NOT NULL
          AND ('|' || "indices" || '|') LIKE :pat
    """)
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL lock_timeout='2s'"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        row = conn.execute(sql, {"pat": pattern}).mappings().first()
    if not row:
        return (0, 0, 0)
    return (int(row["adv"] or 0), int(row["nc"] or 0), int(row["dec"] or 0))

def _get_vals_from_db(index_symbol: str):
    """(advVal, ncVal, decVal) từ asset, tổng totalVal theo state."""
    pattern = f"%|{index_symbol}|%"
    sql = text("""
        SELECT
          SUM(CASE WHEN "matchChange" > 0  THEN COALESCE("totalVal",0) ELSE 0 END)::DOUBLE PRECISION AS "advVal",
          SUM(CASE WHEN "matchChange" = 0  THEN COALESCE("totalVal",0) ELSE 0 END)::DOUBLE PRECISION AS "ncVal",
          SUM(CASE WHEN "matchChange" < 0  THEN COALESCE("totalVal",0) ELSE 0 END)::DOUBLE PRECISION AS "decVal"
        FROM "details"."asset" left join "info"."stock" using("symbol")
        WHERE "indices" IS NOT NULL
          AND ('|' || "indices" || '|') LIKE :pat
    """)
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL lock_timeout='2s'"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        row = conn.execute(sql, {"pat": pattern}).mappings().first()
    if not row:
        return (0.0, 0.0, 0.0)
    return (float(row["advVal"] or 0.0), float(row["ncVal"] or 0.0), float(row["decVal"] or 0.0))

def _get_cefl_counts_from_db(index_symbol: str):
    """
    Trả về (ceil_cnt, floor_cnt): số mã đang kịch trần / kịch sàn theo chỉ số.
    Điều kiện:
      - ceiling: matchPrice = Ceiling AND Ceiling IS NOT NULL
      - floor:   matchPrice = Floor   AND Floor   IS NOT NULL
    """
    pattern = f"%|{index_symbol}|%"
    sql = text("""
        SELECT
          SUM(CASE WHEN "ceiling" IS NOT NULL AND "matchPrice" = "ceiling" THEN 1 ELSE 0 END)::BIGINT AS ceil_cnt,
          SUM(CASE WHEN "floor"   IS NOT NULL AND "matchPrice" = "floor"   THEN 1 ELSE 0 END)::BIGINT AS floor_cnt
        FROM "details"."asset" left join "info"."stock" using("symbol")
        WHERE "indices" IS NOT NULL
          AND ('|' || "indices" || '|') LIKE :pat
    """)
    with engine.connect() as conn:
        conn.execute(text("SET LOCAL lock_timeout='2s'"))
        conn.execute(text("SET LOCAL statement_timeout='5s'"))
        row = conn.execute(sql, {"pat": pattern}).mappings().first()
    if not row:
        return (0, 0)
    return (int(row["ceil_cnt"] or 0), int(row["floor_cnt"] or 0))

def get_counts(index_symbol: str):
    now = time.time()
    ent = _adv_cache.get(index_symbol)
    if ent and (now - ent["ts"] <= ADVDEC_CACHE_TTL):
        return ent["data"]
    data = _get_counts_from_db(index_symbol)
    _adv_cache[index_symbol] = {"ts": now, "data": data}
    return data

def get_vals(index_symbol: str):
    now = time.time()
    ent = _val_cache.get(index_symbol)
    if ent and (now - ent["ts"] <= ADVDEC_CACHE_TTL):
        return ent["data"]
    data = _get_vals_from_db(index_symbol)
    _val_cache[index_symbol] = {"ts": now, "data": data}
    return data

def get_cefl_counts(index_symbol: str):
    now = time.time()
    ent = _cefl_cache.get(index_symbol)
    if ent and (now - ent["ts"] <= ADVDEC_CACHE_TTL):
        return ent["data"]
    data = _get_cefl_counts_from_db(index_symbol)
    _cefl_cache[index_symbol] = {"ts": now, "data": data}
    return data

def _get_refpoint(symbol: str):
    rp = REFPOINT.get(symbol)
    if rp is None:
        log.warning("refPoint for %s not found in refpoint.py -> set None", symbol)
        return None
    try:
        return float(rp)
    except Exception:
        log.warning("Invalid refPoint value for %s in refpoint.py: %r -> set None", symbol, rp)
        return None


# -------------- Stream Handlers --------------
RECONNECT = threading.Event()

def on_error(err):
    log.error("MI stream error: %s", err)
    RECONNECT.set()

def on_close():
    log.warning("Stream closed, will reconnect...")
    RECONNECT.set()

def on_message_MI(message):
    """Nhận MI, giữ NGUYÊN format result và bổ sung advancersDeclinersVal."""
    try:
        data = json.loads(message.get("Content", "{}")) or {}
        raw_symbol = str(data.get("IndexId") or "")
        symbol = _map_symbol(raw_symbol)

        # Counts ưu tiên từ feed
        adv, nc, dec = get_counts(symbol)
        ceil_cnt, floor_cnt = get_cefl_counts(symbol)

        # Vals luôn lấy từ DB (feed thường không có tách theo state)
        advVal, ncVal, decVal = get_vals(symbol)
        if GROUP_KEY == "indices1":
            rp = _get_refpoint(symbol)
        else:
            rp = data.get('PriorIndexValue')

        result ={
                'symbol': symbol,
                'point': data.get('IndexValue'),
                'refPoint': rp,
                'change': data.get('Change'),
                'ratioChange': data.get('RatioChange'),
                'totalMatchVol': data.get('TotalQtty'),
                'totalMatchVal': data.get('TotalValue'),
                'totalDealVol': data.get('TotalQttyPt'),
                'totalDealVal': data.get('TotalValuePt'),
                'totalVol': data.get('AllQty'),
                'totalVal': data.get('AllValue'),
                'advancers': adv,
                'noChanges': nc,
                'decliners': dec,
                'advancersVal': advVal,
                'noChangesVal': ncVal,
                'declinersVal': decVal,
                'ceiling': ceil_cnt,
                'floor': floor_cnt,
            }

        # Publish hợp nhất
        publish(result)

    except Exception:
        log.exception("MI message error")

# -------------- Main loop --------------
def main():
    log.info("Producer MI | stream=%s | publish=%s",
             STREAM_CODE, CHANNEL)

    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    client = MarketDataClient(config)
    while True:
        try:
            mm = MarketDataStream(config, client)
            mm.start(on_message_MI, on_error, STREAM_CODE, on_close)
            woke = RECONNECT.wait(timeout=86400)
            if woke:
                RECONNECT.clear()
        except Exception as e:
            log.error("Stream crashed: %s", e)
            time.sleep(2)

if __name__ == "__main__":
    main()
