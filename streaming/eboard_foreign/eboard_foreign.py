import os, json, time, logging, signal, sys, redis, threading
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

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
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
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
    # if "source" not in payload:
    #     payload["source"] = CHANNEL
    try:
        r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        logging.warning("Redis publish fail (%s): %s", CHANNEL, e)
        try:
            # reconnect Redis
            r = redis.Redis(connection_pool=POOL)
            r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
            logging.info("Redis reconnected and published successfully")
        except Exception as e2:
            logging.error("Redis retry failed: %s", e2)

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