import redis
import os
import json
import pandas as pd
from indicator import caculate_indicators
import math
from collections.abc import Mapping, Sequence
import requests
from upsert_alert import upsert_alert_status

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

            # Không có gì thì bỏ qua, loop tiếp
            if msg is None:
                continue

            # Bỏ qua message hệ thống
            if msg["type"] != "message":
                continue
            
            data_str = msg["data"]
            raw = json.loads(data_str)
            data = raw
             # tick: dict realtime từ streaming
            symbol = data.get("symbol")
            if not (symbol and isinstance(symbol, str) and symbol.isalpha() and len(symbol) == 3):
                continue
             # Lấy history
            df = get_data_from_redis(symbol)
             # Thêm tick mới + tính lại indicator
            (
                df,
                ma10_cross_up, ma20_cross_up, ma50_cross_up, macd_cross_up,
                ma10_cross_down, ma20_cross_down, ma50_cross_down,macd_cross_down,
                ma10_above, ma20_above, ma50_above, macd_above,
                bb_upper_cross_up, bb_upper_cross_down,
                bb_lower_cross_down, bb_lower_cross_up,
                bb_upper_above, bb_lower_below
            ) = caculate_indicators(df, data)
            time_str = df["time"].iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
            status_content = {
                "time": time_str,
                "symbol": symbol,
                "open": df["open"].iloc[-1],
                "high": df["high"].iloc[-1],
                "low": df["low"].iloc[-1],
                "close": df["close"].iloc[-1],
                "volume": df["volume"].iloc[-1],
                "MA10": df["MA10"].iloc[-1],
                "MA20": df["MA20"].iloc[-1],
                "MA50": df["MA50"].iloc[-1],
                'RSI': df['RSI'].iloc[-1],
                'MFI': df['MFI'].iloc[-1],
                'volume_10': df['volume_10'].iloc[-1],
                'volume_20': df['volume_20'].iloc[-1],
                'volume_50': df['volume_50'].iloc[-1],
                "ma10_cross_up": ma10_cross_up,
                "ma20_cross_up": ma20_cross_up,
                "ma50_cross_up": ma50_cross_up,
                "ma10_cross_down": ma10_cross_down,
                "ma20_cross_down": ma20_cross_down,
                "ma50_cross_down": ma50_cross_down,
                "macd1": round(df["MACD"].iloc[-1],2),
                "signal_line1": round(df["Signal_Line"].iloc[-1],2),
                "macd2": round(df["MACD"].iloc[-2],2),
                "signal_line2": round(df["Signal_Line"].iloc[-2],2),
                "macd_cross_up": macd_cross_up,
                "macd_cross_down": macd_cross_down,
                "ma10_above": ma10_above,
                "ma20_above": ma20_above,
                "ma50_above": ma50_above,
                "macd_above": macd_above,
                "bb_upper_cross_up": bb_upper_cross_up,
                "bb_upper_cross_down": bb_upper_cross_down,
                "bb_lower_cross_down": bb_lower_cross_down,
                "bb_lower_cross_up": bb_lower_cross_up,
                "bb_upper_above": bb_upper_above,
                "bb_lower_below": bb_lower_below,
            }
            status_envelope = {"function": "alert_status",
                                "content": status_content,
                                "source": "alert_status",
                            }
            status_json = json.dumps(to_native(status_envelope), ensure_ascii=False)
            # Lưu trạng thái cuối cùng theo symbol
            redis_key = f"alert_status_state:{symbol}"
            r.set(redis_key, status_json)
            # 👉 Publish alert ra pubsub channel "alert_function"
            r.publish("alert_function", status_json)
            upsert_alert_status(status_content)
            print(status_content)
         # ================= TRIGGER: chỉ bắn khi False -> True =================
            # 1. trạng thái trigger hiện tại
            current_trigger_state = {
                "ma10_cross_up": ma10_cross_up,
                "ma20_cross_up": ma20_cross_up,
                "ma50_cross_up": ma50_cross_up,
                "ma10_cross_down": ma10_cross_down,
                "ma20_cross_down": ma20_cross_down,
                "ma50_cross_down": ma50_cross_down,
                "macd_cross_up": macd_cross_up,
                "macd_cross_down": macd_cross_down,
                "bb_upper_cross_up": bb_upper_cross_up,
                "bb_upper_cross_down": bb_upper_cross_down,
                "bb_lower_cross_down": bb_lower_cross_down,
                "bb_lower_cross_up": bb_lower_cross_up,
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
                    "function": "alert_trigger",
                    "content": {
                        "symbol": symbol,
                        "time": time_str,
                        "event": events_to_fire,
                    },
                    "source": "alert_trigger",
                }
                trigger_json = json.dumps(trigger_envelope)
                r.publish("alert_function", trigger_json)
        except Exception as e:
            print(f"Lỗi alert function {symbol}:", e)

if __name__ == "__main__":
    main()
