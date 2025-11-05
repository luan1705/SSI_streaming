# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import configvi as config
from List.upsert import upsert_eboard
from List.exchange import DERIVATIVES
from List.indices_map import indices_map
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
STREAM_CODE = "X:" + "-".join(DERIVATIVES)
CHANNEL = "ebtb_derivatives"
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
        
def find_indices(symbol: str) -> list[str] | None:
    res = [idx for idx, symbols in indices_map.items() if symbol in symbols]
    return res or None

def on_message_X(message):
    try:
        data = json.loads(message.get("Content","{}"))
        sym = data["Symbol"]
        result = {
            "function": "eboard_table",
            "content": {
                "symbol":   sym,
                "exchange": 'DERIVATIVES',
                "indices":  find_indices(sym),
                "ceiling":  data["Ceiling"],
                "floor":    data["Floor"],
                "refPrice": data["RefPrice"],
                "buy": {
                    "price": [data["BidPrice1"], data["BidPrice2"], data["BidPrice3"]],
                    "vol":   [data["BidVol1"], data["BidVol2"], data["BidVol3"]],
                },
                "match": {
                    "price": data["LastPrice"],
                    "vol":   data["LastVol"],
                    "change": data["Change"],
                    "ratioChange": data["RatioChange"],
                },
                "sell": {
                    "price": [data["AskPrice1"], data["AskPrice2"], data["AskPrice3"]],
                    "vol":   [data["AskVol1"], data["AskVol2"], data["AskVol3"]],
                },
                "totalVol": data["TotalVol"],
                "totalVal": data["TotalVal"],
                "high":  data["High"],
                "low":   data["Low"],
                "open":  data["Open"],
                "close": data["Close"],
            }
        }
        # Publish result sang Redis để Hub gom về 1 WS port
        if result["content"]["match"]["ratioChange"] == -100:
            return
        else:
            publish(result)

        # save DB
        c = result["content"]
        indices = c["indices"]
        if isinstance(indices, list):
            indices = "|".join(indices)

        row = {
            "symbol":   c["symbol"],
            "exchange": c["exchange"],
            "indices":  indices,
            "ceiling":  c["ceiling"],
            "floor":    c["floor"],
            "refPrice": c["refPrice"],
            "buyPrice1": c["buy"]["price"][0], "buyVol1": c["buy"]["vol"][0],
            "buyPrice2": c["buy"]["price"][1], "buyVol2": c["buy"]["vol"][1],
            "buyPrice3": c["buy"]["price"][2], "buyVol3": c["buy"]["vol"][2],
            "matchPrice": c["match"]["price"], "matchVol": c["match"]["vol"],
            "matchChange": c["match"]["change"], "matchRatioChange": c["match"]["ratioChange"],
            "sellPrice1": c["sell"]["price"][0], "sellVol1": c["sell"]["vol"][0],
            "sellPrice2": c["sell"]["price"][1], "sellVol2": c["sell"]["vol"][1],
            "sellPrice3": c["sell"]["price"][2], "sellVol3": c["sell"]["vol"][2],
            "totalVol": c["totalVol"], "totalVal": c["totalVal"],
            "high": c["high"], "low": c["low"], "open": c["open"], "close": c["close"],
        }
        # row = {k: (None if (v == 0 or v == "0") else v) for k, v in row.items()}
        upsert_eboard(row)
    except Exception:
        logging.exception("X message error")

RECONNECT = threading.Event()

def on_error(err):
    logging.error(f"X stream error: {err}")
    RECONNECT.set()

def on_close():
    logging.warning("Stream closed, will reconnect...")
    RECONNECT.set()

def main():
    logging.info("Producer X | stream=%s | publish=%s", STREAM_CODE, CHANNEL)

    # graceful shutdown
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    connectssi=MarketDataClient(config)
    while True:
        try:
            mm = MarketDataStream(config,connectssi)
            mm.start(on_message_X, on_error, STREAM_CODE, on_close)
            woke = RECONNECT.wait(timeout=86400)
            if woke:
                RECONNECT.clear()

        except Exception as e:
            logging.error("Stream crashed: %s", e)
            time.sleep(2)

if __name__ == "__main__":
    main()