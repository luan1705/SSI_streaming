import redis
import os
import json
import pandas as pd
from indicator import caculate_indicators

REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL, decode_responses=True,
    socket_timeout=2.5, socket_connect_timeout=2.0,
    health_check_interval=30, max_connections=3, timeout=1.0
)
r = redis.Redis(connection_pool=POOL)


def get_data_from_redis(symbol: str) -> pd.DataFrame:
    redis_key = f"history_tradingview: {symbol}"
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
    pubsub.psubscribe("ebtb_hose*", "ebtb_hnx*", "ebtb_upcom*")

    print("STATUS CONSUMER: listening on streaming:* ...")
    while True:
        try:
            for message in pubsub.listen():
                # Bỏ qua message hệ thống
                if message["type"] != "pmessage":
                    continue
                
                data_str = message["data"]
    
                raw = json.loads(data_str)
                data = raw["content"]
    
                # tick: dict realtime từ streaming
                symbol = data["symbol"]
    
                # Lấy history
                df = get_data_from_redis(symbol)
    
                # Thêm tick mới + tính lại indicator
                (
                    df,
                    close_up_ma10, close_up_ma20, close_up_ma50,
                    close_down_ma10, close_down_ma20, close_down_ma50,
                    macd_cross_up, macd_cross_down,
                    status_up_ma10, status_up_ma20, status_up_ma50, status_up_macd
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
                    "status_up_ma10": status_up_ma10,
                    "status_up_ma20": status_up_ma20,
                    "status_up_ma50": status_up_ma50,
                    "status_up_macd": status_up_macd,
                }

                status_envelope = {"function": "alert_status",
                                    "content": status_content,
                                    "source": "alert_status",
                                }
                status_json = json.dumps(status_envelope)
                # Lưu trạng thái cuối cùng theo symbol
                redis_key = f"alert_status_state:{symbol}"
                r.set(redis_key, status_json)
                # 👉 Publish alert ra pubsub channel "alert_function"
                r.publish("alert_function", status_json)

                # ================= TRIGGER: chỉ bắn khi False -> True =================
                # 1. trạng thái trigger hiện tại
                current_trigger_state = {
                    "close_up_ma10": close_up_ma10,
                    "close_up_ma20": close_up_ma20,
                    "close_up_ma50": close_up_ma50,
                    "close_down_ma10": close_down_ma10,
                    "close_down_ma20": close_down_ma20,
                    "close_down_ma50": close_down_ma50,
                    "macd_cross_up": macd_cross_up,
                    "macd_cross_down": macd_cross_down,
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
            print(f"Lỗi alert function:", e)

if __name__ == "__main__":
    main()
