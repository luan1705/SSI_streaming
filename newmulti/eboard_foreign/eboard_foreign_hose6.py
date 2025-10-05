# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import config
from List.upsert import upsert_r
from List.exchange import HOSE6
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
STREAM_CODE = "R:" + "-".join(HOSE6)
CHANNEL = "ebfr_hose_6"
# --------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
r = redis.from_url(REDIS_URL, decode_responses=True)

def publish(payload: dict):
    """Publish sang Redis để Hub WS (1 port) broadcast."""
    if "source" not in payload:
        payload["source"] = CHANNEL  # nhãn nguồn (client có thể lọc)
    try:
        r.publish(CHANNEL, json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        logging.warning("Redis publish fail (%s): %s", CHANNEL, e)
        
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

def main():
    logging.info("Producer R | stream=%s | publish=%s", STREAM_CODE, CHANNEL)

    # graceful shutdown
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    # Gọi 1 lần, để SDK giữ WS / tự reconnect nội bộ nếu có.
    mm = MarketDataStream(config, MarketDataClient(config))
    mm.start(on_message_R, on_error, STREAM_CODE)

    # Nếu start() trả về bình thường (hiếm), giữ tiến trình sống nhàn:
    try:
        signal.pause()   # Linux/Unix: chờ tín hiệu, không tốn CPU
    except AttributeError:
        while True:
            time.sleep(3600)

if __name__ == "__main__":
    main()