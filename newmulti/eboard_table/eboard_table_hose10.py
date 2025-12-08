# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime , timezone
from zoneinfo import ZoneInfo
import pandas as pd

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import configminh as config
from List.upsert import upsert_eboard, update_price_now
from List.exchange import HOSE10
from List.indices_map import indices_map
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
STREAM_CODE = "X:" + "-".join(HOSE10)
CHANNEL = "ebtb_hose_10"
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

# ---- Helpers & cấu hình tối giản ----
def null0(v):
    return None if (v == 0 or v == "0" or (isinstance(v, float) and abs(v) < 1e-12)) else v

def normalize_buy_sell(content: dict):
    """Chỉ đổi 0 -> null cho BUY/SELL (price & vol)."""
    bs = content.get("buy", {})
    if "price" in bs: bs["price"] = [null0(x) for x in bs["price"]]
    if "vol"   in bs: bs["vol"]   = [null0(x) for x in bs["vol"]]
    ss = content.get("sell", {})
    if "price" in ss: ss["price"] = [null0(x) for x in ss["price"]]
    if "vol"   in ss: ss["vol"]   = [null0(x) for x in ss["vol"]]

# chỉ buy/sell price & vol của row (tự sinh 1..3 để khỏi phải gõ tay)
ROW_ZERO_NULL_FIELDS = (
    {f"buyPrice{i}" for i in range(1,4)} |
    {f"buyVol{i}"   for i in range(1,4)} |
    {f"sellPrice{i}" for i in range(1,4)} |
    {f"sellVol{i}"   for i in range(1,4)}
)



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

def _next_9pm_unix(tz_name: str = "Asia/Ho_Chi_Minh") -> int:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=21, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    return int(target.timestamp())

def save_redis_alert(msg: dict, tz_name: str = "Asia/Ho_Chi_Minh") -> bool:
    """
    Overwrite hoàn toàn (upsert kiểu replace): luôn chỉ còn 1 JSON mới nhất.
    Key: latest_message:{symbol}
    """
    # Lấy symbol từ content
    content = msg.get("content") or {}
    symbol = (content.get("symbol") or "").strip()
    if not symbol:
        logging.warning(f"save_redis_alert: missing symbol in msg={msg}")
        return False

    key = f"latest_message:{symbol}"
    payload = json.dumps(msg, ensure_ascii=False)

    r.set(key, payload)
    # r.expireat(key, _next_9pm_unix(tz_name))
    # logging.info(f"Saved alert to Redis key={key}")
    return True
        
def find_indices(symbol: str) -> list[str] | None:
    res = [idx for idx, symbols in indices_map.items() if symbol in symbols]
    return res or None

def on_message_X(message):
    try:
        data = json.loads(message.get("Content","{}"))
        if data["RatioChange"] == -100:
            return 
        sym = data["Symbol"]
        time = datetime.strptime(data['TradingDate'] + ' ' + data['Time'], '%d/%m/%Y %H:%M:%S')
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        result = {
            "function": "eboard_table",
            "content": {
                'time': time,
                "symbol":   sym,
                "exchange": 'HOSE',
                "indices":  find_indices(sym),
                "ceiling":  data["Ceiling"] / 1000,
                "floor":    data["Floor"] / 1000,
                "refPrice": data["RefPrice"] / 1000,
                "buy": {
                    "price": [data["BidPrice1"]/1000, data["BidPrice2"]/1000, data["BidPrice3"]/1000],
                    "vol":   [data["BidVol1"], data["BidVol2"], data["BidVol3"]],
                },
                "match": {
                    "price": data["LastPrice"]/1000,
                    "vol":   data["LastVol"],
                    "change": data["Change"]/1000,
                    "ratioChange": data["RatioChange"],
                },
                "sell": {
                    "price": [data["AskPrice1"]/1000, data["AskPrice2"]/1000, data["AskPrice3"]/1000],
                    "vol":   [data["AskVol1"], data["AskVol2"], data["AskVol3"]],
                },
                "totalVol": data["TotalVol"],
                "totalVal": data["TotalVal"],
                "high":  data["High"]/1000,
                "low":   data["Low"]/1000,
                "open":  data["Open"]/1000,
                "close": data["Close"]/1000,

            }
        }
        normalize_buy_sell(result["content"])
        # Publish result sang Redis để Hub gom về 1 WS port
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
        row = {k: (null0(v) if k in ROW_ZERO_NULL_FIELDS else v) for k, v in row.items()}
        upsert_eboard(row)
        save_redis_alert(result)

        price_now ={"symbol":   sym,
                    "price_now": data["LastPrice"]/1000}
        update_price_now(price_now)

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