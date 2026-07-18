# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
from notify import notify

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import configvi as config
#from List.upsert import upsert_eboard, update_price_now
from List.exchange import DERIVATIVES
from List.indices_map import indices_map
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://root:Dnl_123456@tanhungsoft.com:6379"
STREAM_CODE = "X:" + "-".join(DERIVATIVES)
CHANNEL = "asset"
ACTIVE_CHANNEL = "active"
ALERT_FUNCTION_CHANNEL = os.getenv("ALERT_FUNCTION_CHANNEL", "alert_function")
# --------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
POOL = None
r = None

def create_redis():
    global POOL, r

    if POOL:
        try:
            POOL.disconnect()
        except Exception:
            pass

    POOL = redis.BlockingConnectionPool.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
        max_connections=10,
        timeout=1.0,
    )

    r = redis.Redis(connection_pool=POOL)
    return r

def reconnect_redis():
    global r

    logging.warning("Reconnecting Redis...")

    try:
        r = create_redis()
        r.ping()
        logging.info("Redis reconnect OK")
        return True

    except Exception as e:
        logging.error("Redis reconnect failed: %s", e)
        return False

# init lần đầu
r = create_redis()

# ---- Helpers & cấu hình tối giản ----
def null0(v):
    """Chỉ đổi 0 -> None (giữ nguyên các giá trị khác)."""
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

def normalize_buy_sell(content: dict):
    """
    CHỈ xử lý BUY/SELL level 1..3 (giá + vol) theo dạng field phẳng:
    buyPrice1..3, buyVol1..3, sellPrice1..3, sellVol1..3
    """
    for i in range(1, 4):
        bp = f"buyPrice{i}";  bv = f"buyVol{i}"
        sp = f"sellPrice{i}"; sv = f"sellVol{i}"

        if bp in content: content[bp] = null0(content.get(bp))
        if bv in content: content[bv] = null0(content.get(bv))
        if sp in content: content[sp] = null0(content.get(sp))
        if sv in content: content[sv] = null0(content.get(sv))



def publish(payload: dict, channel: str = CHANNEL):
    global r
    data = json.dumps(payload, ensure_ascii=False)

    for attempt in range(1, 4):  # thử tối đa 3 lần
        try:
            r.publish(channel, data)
            return  # thành công → thoát
        except Exception as e:
            logging.warning("Redis publish fail (%s) attempt %d/3: %s", channel, attempt, e)
            if attempt == 2:
                try:
                    notify(f"⚠️ [derivatives] Redis publish fail ({channel}) attempt 2/3", level="warning")
                except Exception:
                    pass
            reconnect_redis()
            time.sleep(1)

    # hết 3 lần vẫn lỗi
    logging.error("Redis publish give up (%s), exiting...", channel)
    try:
        notify(f"🔴 [derivatives] Redis publish give up ({channel}), restarting...", level="error")
    except Exception:
        pass
    os._exit(1)
        
def find_indices(symbol: str) -> list[str] | None:
    res = [idx for idx, symbols in indices_map.items() if symbol in symbols]
    return res or None

def on_message_X(message):
    sym=""
    try:
        data = json.loads(message.get("Content","{}"))
        sym = data["Symbol"]
        ratio_change = None if data.get("RatioChange") == -100 else data.get("RatioChange")
        result ={
                "symbol":   sym,
                # "exchange": 'DERIVATIVES',
                # "indices":  find_indices(sym),
                "ceiling":  data["Ceiling"],
                "floor":    data["Floor"],
                "refPrice": data["RefPrice"],
                "buyPrice3": data["BidPrice3"],
                "buyVol3":   data["BidVol3"],
                "buyPrice2": data["BidPrice2"],
                "buyVol2":   data["BidVol2"],
                "buyPrice1": data["BidPrice1"],
                "buyVol1":   data["BidVol1"],
                "matchPrice": data["LastPrice"],
                "matchVol":   data["LastVol"],
                "matchChange": round(data["Change"], 2),
                "matchRatioChange": ratio_change,
                "sellPrice1": data["AskPrice1"],
                "sellVol1":   data["AskVol1"],
                "sellPrice2": data["AskPrice2"],
                "sellVol2":   data["AskVol2"],
                "sellPrice3": data["AskPrice3"],
                "sellVol3":   data["AskVol3"],
                "totalVol": data["TotalVol"],
                "totalVal": data["TotalVal"],
                "high":  data["High"],
                "low":   data["Low"],
                "open":  data["Open"],
                "close": data["Close"],
            }
        active = {
                "symbol":   sym,
                "active": True
        }
        normalize_buy_sell(result)
        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result, CHANNEL)
        publish(result, ALERT_FUNCTION_CHANNEL)
        publish(active, ACTIVE_CHANNEL)

    except Exception:
        logging.exception("X message error - %s", sym)

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
            time.sleep(1)

if __name__ == "__main__":
    main()
