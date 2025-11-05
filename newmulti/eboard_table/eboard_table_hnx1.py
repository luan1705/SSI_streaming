# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo
import pandas as pd

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import configvi as config
from List.upsert import upsert_eboard
from List.exchange import HNX1
from List.indices_map import indices_map
import threading
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
STREAM_CODE = "X:" + "-".join(HNX1)
CHANNEL = "ebtb_hnx_1"
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

def indicators(data, new_close, new_volume):
    # Thêm dữ liệu mới vào DataFrame
    new_data = pd.DataFrame({'close': [new_close], 'volume': [new_volume]})
    data = pd.concat([data, new_data], ignore_index=True)

    # tính MA 10,20,50
    data['MA10'] = round(data['close'].rolling(window=10).mean(),2)
    data['MA20'] = round(data['close'].rolling(window=20).mean(),2)
    data['MA50'] = round(data['close'].rolling(window=50).mean(),2)

    #tính MACD
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['histogram'] = data['MACD'] - data['signal_Line']

    #tính volume TB 10,20,50
    data['volume_10'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_20'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_50'] = round(data['volume'].rolling(50).mean(),2)
    
    # tính RSI
    delta = data['close'].diff()
    avg_gain = delta.where(delta > 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    avg_loss = -delta.where(delta < 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = round((100 - (100/(1 + rs))),2)

    # tính MFI
    tp = (data['close'] + data['close'] + data['close']) / 3
    mf = tp * data['volume'] 
    positive_mf = mf.where(tp.diff() > 0, 0)
    negative_mf = mf.where(tp.diff() < 0, 0)
    positive_mf_sum = positive_mf.rolling(window=14).sum()
    negative_mf_sum = negative_mf.rolling(window=14).sum()
    mfr = positive_mf_sum / negative_mf_sum
    data['MFI'] = round((100 - (100 / (1 + mfr))),2)


    close_up_ma10 = False
    close_up_ma20 = False
    close_up_ma50 = False

    close_down_ma10 = False
    close_down_ma20 = False
    close_down_ma50 = False

    macd_cross_up = False
    macd_cross_down = False

    # MA cắt lên
    if data['close'].iloc[-2] < data['MA20'].iloc[-2] and data['close'].iloc[-1] >= data['MA20'].iloc[-1]:
        close_up_ma10 = True
    if data['close'].iloc[-2] < data['MA20'].iloc[-2] and data['close'].iloc[-1]  >= data['MA20'].iloc[-1]:
        close_up_ma20 = True
    if data['close'].iloc[-2] < data['MA50'].iloc[-2] and data['close'].iloc[-1]  >= data['MA50'].iloc[-1]:
        close_up_ma20 = True

    # MA cắt xuống
    if data['close'].iloc[-2] > data['MA20'].iloc[-2] and data['close'].iloc[-1]  <= data['MA20'].iloc[-1]:
        close_down_ma10 = True
    if data['close'].iloc[-2] > data['MA20'].iloc[-2] and data['close'].iloc[-1]  <= data['MA20'].iloc[-1]:
        close_down_ma20 = True
    if data['close'].iloc[-2] > data['MA50'].iloc[-2] and data['close'].iloc[-1]  <= data['MA50'].iloc[-1]:
        close_down_ma50 = True

    #MACD cắt lên
    if data['MACD'].iloc[-2] < data['signal_Line'].iloc[-2] and data['MACD'].iloc[-1] >= data['signal_Line'].iloc[-1]:
        macd_cross_up = True

    #MACD cắt xuống
    if data['MACD'].iloc[-2] > data['signal_Line'].iloc[-2] and data['MACD'].iloc[-1] <= data['signal_Line'].iloc[-1]:
        macd_cross_down = False

    return data, close_up_ma10, close_up_ma20, close_up_ma50, close_down_ma10, close_down_ma20, close_down_ma50, macd_cross_up, macd_cross_down

def get_data_from_redis(symbol):
    try:
        redis_key = f"history_tradingview: {symbol}"
        values = r.lrange(redis_key, 0, -1)

        # Parse JSON thành dict
        records = [json.loads(v) for v in values]

        # Tạo DataFrame
        df = pd.DataFrame(records)
        df['close'] = df['close'].astype(float)
        df['time'] = pd.to_datetime(df['time'])
        return df
    except Exception as e:
        print(f"Lỗi Redis với {symbol}: {e}")
        return None

def indicators(data, new_close, new_volume):
    # Thêm dữ liệu mới vào DataFrame
    new_data = pd.DataFrame({'close': [new_close], 'volume': [new_volume]})
    data = pd.concat([data, new_data], ignore_index=True)

    # tính MA 10,20,50
    data['MA10'] = round(data['close'].rolling(window=10).mean(),2)
    data['MA20'] = round(data['close'].rolling(window=20).mean(),2)
    data['MA50'] = round(data['close'].rolling(window=50).mean(),2)

    #tính MACD
    data['EMA_12'] = data['close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['signal_Line'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['histogram'] = data['MACD'] - data['signal_Line']

    #tính volume TB 10,20,50
    data['volume_10'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_20'] = round(data['volume'].rolling(20).mean(),2)
    data['volume_50'] = round(data['volume'].rolling(50).mean(),2)
    
    # tính RSI
    delta = data['close'].diff()
    avg_gain = delta.where(delta > 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    avg_loss = -delta.where(delta < 0, 0).ewm(alpha=1/14, min_periods=1, adjust=False).mean()
    rs = avg_gain / avg_loss
    data['RSI'] = round((100 - (100/(1 + rs))),2)

    # tính MFI
    tp = (data['close'] + data['close'] + data['close']) / 3
    mf = tp * data['volume'] 
    positive_mf = mf.where(tp.diff() > 0, 0)
    negative_mf = mf.where(tp.diff() < 0, 0)
    positive_mf_sum = positive_mf.rolling(window=14).sum()
    negative_mf_sum = negative_mf.rolling(window=14).sum()
    mfr = positive_mf_sum / negative_mf_sum
    data['MFI'] = round((100 - (100 / (1 + mfr))),2)


    close_up_ma10 = False
    close_up_ma20 = False
    close_up_ma50 = False

    close_down_ma10 = False
    close_down_ma20 = False
    close_down_ma50 = False

    macd_cross_up = False
    macd_cross_down = False

    # MA cắt lên
    if data['close'].iloc[-2] < data['MA20'].iloc[-2] and data['close'].iloc[-1] >= data['MA20'].iloc[-1]:
        close_up_ma10 = True
    if data['close'].iloc[-2] < data['MA20'].iloc[-2] and data['close'].iloc[-1]  >= data['MA20'].iloc[-1]:
        close_up_ma20 = True
    if data['close'].iloc[-2] < data['MA50'].iloc[-2] and data['close'].iloc[-1]  >= data['MA50'].iloc[-1]:
        close_up_ma20 = True

    # MA cắt xuống
    if data['close'].iloc[-2] > data['MA20'].iloc[-2] and data['close'].iloc[-1]  <= data['MA20'].iloc[-1]:
        close_down_ma10 = True
    if data['close'].iloc[-2] > data['MA20'].iloc[-2] and data['close'].iloc[-1]  <= data['MA20'].iloc[-1]:
        close_down_ma20 = True
    if data['close'].iloc[-2] > data['MA50'].iloc[-2] and data['close'].iloc[-1]  <= data['MA50'].iloc[-1]:
        close_down_ma50 = True

    #MACD cắt lên
    if data['MACD'].iloc[-2] < data['signal_Line'].iloc[-2] and data['MACD'].iloc[-1] >= data['signal_Line'].iloc[-1]:
        macd_cross_up = True

    #MACD cắt xuống
    if data['MACD'].iloc[-2] > data['signal_Line'].iloc[-2] and data['MACD'].iloc[-1] <= data['signal_Line'].iloc[-1]:
        macd_cross_down = False

    return data, close_up_ma10, close_up_ma20, close_up_ma50, close_down_ma10, close_down_ma20, close_down_ma50, macd_cross_up, macd_cross_down

def get_data_from_redis(symbol):
    try:
        redis_key = f"history_tradingview: {symbol}"
        values = r.lrange(redis_key, 0, -1)

        # Parse JSON thành dict
        records = [json.loads(v) for v in values]

        # Tạo DataFrame
        df = pd.DataFrame(records)
        df['close'] = df['close'].astype(float)
        df['time'] = pd.to_datetime(df['time'])
        return df
    except Exception as e:
        print(f"Lỗi Redis với {symbol}: {e}")
        return None

def on_message_X(message):
    try:
        data = json.loads(message.get("Content","{}"))
        sym = data["Symbol"]
        stock = get_data_from_redis(sym)
        result, close_up_ma10, close_up_ma20, close_up_ma50, close_down_ma10, close_down_ma20, close_down_ma50, \
        macd_cross_up, macd_cross_down = indicators(
                stock,
                data['Close'] / 1000,
                data['TotalVol']
            )
        time = datetime.strptime(data['TradingDate'] + ' ' + data['Time'], '%d/%m/%Y %H:%M:%S')
        time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        result = {
            "function": "eboard_table",
            "content": {
                'time': time,
                "symbol":   sym,
                "exchange": 'HNX',
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

                'Side': data['Side'],

                # dữ liệu chỉ báo
                'close_up_ma10': close_up_ma10,
                'close_up_ma20': close_up_ma20,
                'close_up_ma50': close_up_ma50,
                'close_down_ma10': close_down_ma10,
                'close_down_ma20': close_down_ma20,
                'close_down_ma50': close_down_ma50,
                'macd_cross_up': macd_cross_up,
                'macd_cross_down': macd_cross_down,
                'RSI': result['RSI'].iloc[-1],
                'MFI': result['MFI'].iloc[-1],
                'MA10': result['MA10'].iloc[-1],
                'MA20': result['MA20'].iloc[-1],
                'MA50': result['MA50'].iloc[-1],
                'volume_10': result['volume_10'].iloc[-1],
                'volume_20': result['volume_20'].iloc[-1],
                'volume_50': result['volume_50'].iloc[-1],
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