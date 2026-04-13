import redis
import os
import json
import pandas as pd
from indicator import calculate_indicators
import math
from collections.abc import Mapping, Sequence
import requests
from upsert_alert import upsert_alert_status
# from SSI_streaming.streaming.List.indices_map import indices_map
# from SSI_streaming.streaming.List.exchange import EXCHANGE_LISTS

WEBHOOK_URL = "https://n8n.videv.cloud/webhook/redis_alert" 

# ✅ NEW: hàm đổi NaN / inf -> None (lúc json.dumps sẽ ra null)
def to_native(o):
    if isinstance(o, Mapping):
        return {k: to_native(v) for k, v in o.items()}

    if isinstance(o, Sequence) and not isinstance(o, (str, bytes, bytearray)):
        return [to_native(x) for x in o]

    if isinstance(o, float):
        if math.isnan(o) or math.isinf(o):
            return None

    return o


REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL, decode_responses=True,
    socket_timeout=60, socket_connect_timeout=5,
    health_check_interval=30, max_connections=3, timeout=1.0
)
r = redis.Redis(connection_pool=POOL)


def get_data_from_redis(symbol: str) -> pd.DataFrame:
    redis_key = f"ohlcv:{symbol}"
    values = r.lrange(redis_key, 0, -1)

    if not values:
        # Không có history thì trả về DataFrame rỗng đúng cột
        return pd.DataFrame(columns=["time", "symbol", "open", "high", "low", "close", "volume"])

    records = [json.loads(v) for v in values]
    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time"])
    return df


def main():
    pubsub = r.pubsub()
    pubsub.subscribe("asset")
    print("STATUS CONSUMER: listening on streaming:* ...")
    while True:
        try:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            if msg is None:
                continue

            if msg["type"] != "message":
                continue

            latest_by_symbol = {}

            # message đầu tiên
            try:
                raw = json.loads(msg["data"])
                symbol = raw.get("symbol")
                if symbol and isinstance(symbol, str) and symbol.isalpha() and len(symbol) == 3:
                    latest_by_symbol[symbol] = raw
            except Exception:
                continue

            # hút nhanh backlog hiện có, chỉ giữ message mới nhất theo symbol
            while True:
                next_msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.0)
                if next_msg is None:
                    break
                if next_msg["type"] != "message":
                    continue

                try:
                    raw = json.loads(next_msg["data"])
                    symbol = raw.get("symbol")
                    if symbol and isinstance(symbol, str) and symbol.isalpha() and len(symbol) == 3:
                        latest_by_symbol[symbol] = raw
                except Exception:
                    continue

            for symbol, data in latest_by_symbol.items():
                # Lấy history
                df = get_data_from_redis(symbol)
                # Thêm tick mới + tính lại indicator
                (
                    df, EMA10, EMA20, EMA50, SMA10, SMA20, SMA50, RSI, MFI,

                    BB_upper, BB_lower,

                    volume_10, volume_20, volume_50,

                    close_cross_up_sma10, close_cross_up_sma20, close_cross_up_sma50,
                    close_cross_down_sma10, close_cross_down_sma20, close_cross_down_sma50,

                    pivot_cross_up_sma10, pivot_cross_up_sma20, pivot_cross_up_sma50,
                    pivot_cross_down_sma10, pivot_cross_down_sma20, pivot_cross_down_sma50,

                    sma10_cross_up_sma20, sma10_cross_down_sma20,
                    sma10_cross_up_sma50, sma10_cross_down_sma50,
                    sma20_cross_up_sma50, sma20_cross_down_sma50,

                    close_cross_up_ema10, close_cross_up_ema20, close_cross_up_ema50,
                    close_cross_down_ema10, close_cross_down_ema20, close_cross_down_ema50,

                    pivot_cross_up_ema10, pivot_cross_up_ema20, pivot_cross_up_ema50,
                    pivot_cross_down_ema10, pivot_cross_down_ema20, pivot_cross_down_ema50,

                    ema10_cross_up_ema20, ema10_cross_down_ema20,
                    ema10_cross_up_ema50, ema10_cross_down_ema50,
                    ema20_cross_up_ema50, ema20_cross_down_ema50,

                    macd_cross_up_signal, macd_cross_down_signal,
                    macd_cross_up_zero, macd_cross_down_zero,

                    close_cross_up_bb_upper, close_cross_down_bb_upper,
                    close_cross_up_bb_lower, close_cross_down_bb_lower,

                    pivot_cross_up_bb_upper, pivot_cross_down_bb_upper,
                    pivot_cross_up_bb_lower, pivot_cross_down_bb_lower,

                    stoch_cross_up, stoch_cross_down,

                    tk_cross_up_ks, tk_cross_down_ks,

                    close_cross_up_cloud, close_cross_down_cloud,
                    pivot_cross_up_cloud, pivot_cross_down_cloud
                ) = calculate_indicators(df, data)
                
                if df.empty or "time" not in df.columns:
                    print(f"{symbol}: missing 'time' column after caculate_indicators")
                    continue

                time_val = pd.to_datetime(df["time"].iloc[-1], errors="coerce")
                if pd.isna(time_val):
                    print(f"{symbol}: invalid time value")
                    continue

                time_str = time_val.strftime("%Y-%m-%d %H:%M:%S")

                status_content = {
                    "symbol": symbol,
                    "time": time_str,

                    "open": df["open"].iloc[-1],
                    "high": df["high"].iloc[-1],
                    "low": df["low"].iloc[-1],
                    "close": df["close"].iloc[-1],
                    "volume": df["volume"].iloc[-1],
                    "pivot": df["pivot"].iloc[-1],

                    "sma10": round(SMA10, 2),
                    "sma20": round(SMA20, 2),
                    "sma50": round(SMA50, 2),

                    "ema10": round(EMA10, 2),
                    "ema20": round(EMA20, 2),
                    "ema50": round(EMA50, 2),

                    "RSI": round(RSI, 2),
                    "MFI": round(MFI, 2),

                    "macd": round(df["MACD"].iloc[-1], 2),
                    "signal_line": round(df["Signal_Line"].iloc[-1],2),

                    "BB_upper": round(BB_upper,2),
                    "BB_lower": round(BB_lower,2),
                    
                    # "macd1": round(df["MACD"].iloc[-1],5),
                    # "signal_line1": round(df["Signal_Line"].iloc[-1],5),
                    # "macd2": round(df["MACD"].iloc[-2],5),
                    # "signal_line2": round(df["Signal_Line"].iloc[-2],5),
                                    
                    'volume_10': round(volume_10, 2),
                    'volume_20': round(volume_20, 2),
                    'volume_50': round(volume_50, 2),

                    "close_cross_up_sma10": close_cross_up_sma10,
                    "close_cross_up_sma20": close_cross_up_sma20,
                    "close_cross_up_sma50": close_cross_up_sma50,
                    "close_cross_down_sma10": close_cross_down_sma10,
                    "close_cross_down_sma20": close_cross_down_sma20,
                    "close_cross_down_sma50": close_cross_down_sma50,

                    "pivot_cross_up_sma10": pivot_cross_up_sma10,
                    "pivot_cross_up_sma20": pivot_cross_up_sma20,
                    "pivot_cross_up_sma50": pivot_cross_up_sma50,
                    "pivot_cross_down_sma10": pivot_cross_down_sma10,
                    "pivot_cross_down_sma20": pivot_cross_down_sma20,
                    "pivot_cross_down_sma50": pivot_cross_down_sma50,

                    "sma10_cross_up_sma20": sma10_cross_up_sma20,
                    "sma10_cross_down_sma20": sma10_cross_down_sma20,
                    "sma10_cross_up_sma50": sma10_cross_up_sma50,
                    "sma10_cross_down_sma50": sma10_cross_down_sma50,
                    "sma20_cross_up_sma50": sma20_cross_up_sma50,
                    "sma20_cross_down_sma50": sma20_cross_down_sma50,

                    "close_cross_up_ema10": close_cross_up_ema10,
                    "close_cross_up_ema20": close_cross_up_ema20,
                    "close_cross_up_ema50": close_cross_up_ema50,
                    "close_cross_down_ema10": close_cross_down_ema10,
                    "close_cross_down_ema20": close_cross_down_ema20,
                    "close_cross_down_ema50": close_cross_down_ema50,

                    "pivot_cross_up_ema10": pivot_cross_up_ema10,
                    "pivot_cross_up_ema20": pivot_cross_up_ema20,
                    "pivot_cross_up_ema50": pivot_cross_up_ema50,
                    "pivot_cross_down_ema10": pivot_cross_down_ema10,
                    "pivot_cross_down_ema20": pivot_cross_down_ema20,
                    "pivot_cross_down_ema50": pivot_cross_down_ema50,

                    "ema10_cross_up_ema20": ema10_cross_up_ema20,
                    "ema10_cross_down_ema20": ema10_cross_down_ema20,
                    "ema10_cross_up_ema50": ema10_cross_up_ema50,
                    "ema10_cross_down_ema50": ema10_cross_down_ema50,
                    "ema20_cross_up_ema50": ema20_cross_up_ema50,
                    "ema20_cross_down_ema50": ema20_cross_down_ema50,

                    "macd_cross_up_signal": macd_cross_up_signal,
                    "macd_cross_down_signal": macd_cross_down_signal,
                    "macd_cross_up_zero": macd_cross_up_zero,
                    "macd_cross_down_zero": macd_cross_down_zero,

                    "close_cross_up_bb_upper": close_cross_up_bb_upper,
                    "close_cross_down_bb_upper": close_cross_down_bb_upper,
                    "close_cross_up_bb_lower": close_cross_up_bb_lower,
                    "close_cross_down_bb_lower": close_cross_down_bb_lower,

                    "pivot_cross_up_bb_upper": pivot_cross_up_bb_upper,
                    "pivot_cross_down_bb_upper": pivot_cross_down_bb_upper,
                    "pivot_cross_up_bb_lower": pivot_cross_up_bb_lower,
                    "pivot_cross_down_bb_lower": pivot_cross_down_bb_lower,

                    "stoch_cross_up": stoch_cross_up,
                    "stoch_cross_down": stoch_cross_down,

                    "tk_cross_up_ks": tk_cross_up_ks,
                    "tk_cross_down_ks": tk_cross_down_ks,

                    "close_cross_up_cloud": close_cross_up_cloud,
                    "close_cross_down_cloud": close_cross_down_cloud,
                    "pivot_cross_up_cloud": pivot_cross_up_cloud,
                    "pivot_cross_down_cloud": pivot_cross_down_cloud,
                }
                status_envelope = status_content
                status_json = json.dumps(to_native(status_envelope), ensure_ascii=False)
                # Lưu trạng thái cuối cùng theo symbol
                redis_key = f"alert_status_state:{symbol}"
                r.set(redis_key, status_json)
                # 👉 Publish alert ra pubsub channel "alert_status"
                r.publish("alert_status", status_json)
                upsert_alert_status(status_content)
                print(status_content)
            # ================= TRIGGER: chỉ bắn khi False -> True =================
                # 1. trạng thái trigger hiện tại
                current_trigger_state = {
                    "close_cross_up_sma10": close_cross_up_sma10,
                    "close_cross_up_sma20": close_cross_up_sma20,
                    "close_cross_up_sma50": close_cross_up_sma50,
                    "close_cross_down_sma10": close_cross_down_sma10,
                    "close_cross_down_sma20": close_cross_down_sma20,
                    "close_cross_down_sma50": close_cross_down_sma50,

                    "close_cross_up_ema10": close_cross_up_ema10,
                    "close_cross_up_ema20": close_cross_up_ema20,
                    "close_cross_up_ema50": close_cross_up_ema50,
                    "close_cross_down_ema10": close_cross_down_ema10,
                    "close_cross_down_ema20": close_cross_down_ema20,
                    "close_cross_down_ema50": close_cross_down_ema50,

                    "macd_cross_up_signal": macd_cross_up_signal,
                    "macd_cross_down_signal": macd_cross_down_signal,
                    "macd_cross_up_zero": macd_cross_up_zero,
                    "macd_cross_down_zero": macd_cross_down_zero,

                    "close_cross_up_bb_upper": close_cross_up_bb_upper,
                    "close_cross_down_bb_upper": close_cross_down_bb_upper,
                    "close_cross_up_bb_lower": close_cross_up_bb_lower,
                    "close_cross_down_bb_lower": close_cross_down_bb_lower,

                    "close_cross_up_cloud": close_cross_up_cloud,
                    "close_cross_down_cloud": close_cross_down_cloud,
                }
                trigger_state_key = f"alert_trigger_state:{symbol}"
                prev_state_json = r.get(trigger_state_key)
                if prev_state_json:
                    prev_trigger_state = json.loads(prev_state_json)
                else:
                    prev_trigger_state = {}
            # 2. tìm event nào từ False -> True
                events_to_fire = []
                for ev_name, cur_val in current_trigger_state.items():
                    prev_val = prev_trigger_state.get(ev_name, False)
                    if (not prev_val) and cur_val:
                        events_to_fire.append(ev_name)
            # 3. luôn lưu trạng thái mới nhất vào alert_trigger_state (đều đều)
                r.set(trigger_state_key, json.dumps(current_trigger_state))
            # 4. nếu có event mới chuyển từ False -> True thì mới publish
                if events_to_fire:
                    trigger_envelope = {
                            "symbol": symbol,
                            "time": time_str,
                            "event": events_to_fire,
                        }
                    trigger_json = json.dumps(trigger_envelope)
                    r.publish("alert_trigger", trigger_json)
        except Exception as e:
            print(f"Lỗi alert function {symbol}:", e)

if __name__ == "__main__":
    main()
