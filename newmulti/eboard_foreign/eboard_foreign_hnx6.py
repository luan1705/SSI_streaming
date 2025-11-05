# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import confighao as config
from List.upsert import update_eboard
from List.exchange import HNX6
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
STREAM_CODE = "R:" + "-".join(HNX6)
CHANNEL = "ebfr_hnx_6"
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
    if "source" not in payload:
        payload["source"] = CHANNEL
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
        
def on_message_R(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['Symbol']
        result = {
            'function': 'eboard_foreign',
            'content': {
                'symbol': symbol,
                'buyVol': data['BuyVol'],
                'sellVol': data['SellVol'],
                'room': data['CurrentRoom'],
                'buyVal': data['BuyVal'] ,
                'sellVal': data['SellVal']
            }
        }
        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result)

        # save DB
        c = result["content"]
        row = {
            "symbol": c["symbol"],
            "foreignBuyVol":    c["buyVol"],
            "foreignSellVol":   c["sellVol"],
            "foreignRoom":   c["room"],
            "foreignBuyVal":    c["buyVal"],
            "foreignSellVal":   c["sellVal"],
        }
        # row = {k: (None if (v == 0 or v == "0") else v) for k, v in row.items()}
        update_eboard(row)
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
            time.sleep(2)

if __name__ == "__main__":
    main()