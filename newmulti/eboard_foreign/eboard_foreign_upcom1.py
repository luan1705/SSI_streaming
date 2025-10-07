# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import config
from List.upsert import upsert_r
from List.exchange import UPCOM1
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@localhost:6379/1"
STREAM_CODE = "R:" + "-".join(UPCOM1)
CHANNEL = "ebfr_upcom_1"
# --------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
POOL = redis.ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,           # timeout đọc/ghi
    socket_connect_timeout=2.0,   # timeout connect
    health_check_interval=30,     # ping định kỳ 30s
    retry_on_timeout=True,        # retry nhẹ nội bộ
    max_connections=50,
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
            "buyVol":    c["buyVol"],
            "sellVol":   c["sellVol"],
            "room":   c["room"],
            "buyVal":    c["buyVal"],
            "sellVal":   c["sellVal"],
        }
        upsert_r(row)
    except Exception:
        logging.exception("R message error")

def on_error(err):
    logging.error(f"R stream error: {err}")
    raise Exception(err)

def on_close():
    logging.warning("Stream closed, will reconnect...")

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
        except Exception as e:
            logging.error("Stream crashed: %s", e)

        logging.info("Disconnected (goodbye). Reconnect in 5s...")
        time.sleep(2)

if __name__ == "__main__":
    main()