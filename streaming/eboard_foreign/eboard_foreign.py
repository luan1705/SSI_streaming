import os, json, time, logging, signal, sys, redis, threading
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from notify import notify

from List import confighao as config
#from List.upsert import update_eboard
from List.exchange import EBOARD_GROUPS
# =========================================
GROUP_KEY = os.getenv("EBOARD_GROUP")
if not GROUP_KEY:
    raise RuntimeError("Missing ENV EBOARD_GROUP (vd: hose1/hnx1/upcom1/cw/hnxbond/etfhose/derivatives)")

SYMBOL_LIST = EBOARD_GROUPS.get(GROUP_KEY)
if not SYMBOL_LIST:
    raise RuntimeError(f"Unknown EBOARD_GROUP={GROUP_KEY}. Check EBOARD_GROUPS in List.exchange")

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://root:Dnl_123456@tanhungsoft.com:6379"
STREAM_CODE = "R:" + "-".join(SYMBOL_LIST)
CHANNEL = "asset"
# --------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,           # timeout đọc/ghi
    socket_connect_timeout=2.0,   # timeout connect
    health_check_interval=30,     # ping định kỳ 30s
    max_connections=3,            # Mỗi container chỉ tối đa 3 socket tới Redis
    timeout=1.0,                  # Khi pool bận, chờ tối đa 1s để lấy connection (không drop)
)
r = redis.Redis(connection_pool=POOL)

def publish(payload: dict):
    global r
    data = json.dumps(payload, ensure_ascii=False)

    for attempt in range(1, 4):  # thử tối đa 3 lần
        try:
            r.publish(CHANNEL, data)
            return  # thành công → thoát
        except Exception as e:
            logging.warning("Redis publish fail (%s) attempt %d/3: %s", CHANNEL, attempt, e)
            if attempt == 2:
                try:
                    notify(f"⚠️ [{GROUP_KEY}] Redis publish fail ({CHANNEL}) attempt 2/3", level="warning")
                except Exception:
                    pass
            try:
                r = redis.Redis(connection_pool=POOL)
                r.ping()
                logging.info("Redis reconnect OK")
            except Exception as re:
                logging.error("Redis reconnect failed: %s", re)
            time.sleep(1)

    # hết 3 lần vẫn lỗi
    logging.error("Redis publish give up (%s), exiting...", CHANNEL)
    try:
        notify(f"🔴 [{GROUP_KEY}] Redis publish give up ({CHANNEL}), restarting...", level="error")
    except Exception:
        pass
    os._exit(1)

def null0(v):
    """0 -> None (null). Giữ nguyên các giá trị khác."""
    try:
        if v is None:
            return None
        if v == 0 or v == "0":
            return None
        if isinstance(v, float) and abs(v) < 1e-12:
            return None
        return v
    except Exception:
        return v
        
def on_message_R(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['Symbol']
        result = {
                'symbol': symbol,
                "foreignBuyVol":  null0(data.get("BuyVol")),
                "foreignSellVol": null0(data.get("SellVol")),
                "foreignRoom":    null0(data.get("CurrentRoom")),
                "foreignBuyVal":  null0(data.get("BuyVal")),
                "foreignSellVal": null0(data.get("SellVal")),
            }
        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result)

    except Exception:
        logging.exception("R message error")

RECONNECT = threading.Event()

def on_error(err):
    logging.error(f"R stream error: {err}")
    RECONNECT.set()

def on_close():
    logging.warning("Stream closed, will reconnect...")
    RECONNECT.set()

def main():
    logging.info("Producer R | stream=%s | publish=%s", STREAM_CODE, CHANNEL)

    # graceful shutdown
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    connectssi=MarketDataClient(config)
    while True:
        try:
            mm = MarketDataStream(config,connectssi)
            mm.start(on_message_R, on_error, STREAM_CODE, on_close)
            woke = RECONNECT.wait(timeout=86400)
            if woke:
                RECONNECT.clear()
        except Exception as e:
            logging.error("Stream crashed: %s", e)
            time.sleep(1)

if __name__ == "__main__":
    main()