import redis
import pandas as pd
import json
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from List.exchange import total_list

# Kết nối DB
engine = create_engine('postgresql://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech')

# Kết nối Redis
REDIS_URL   = "redis://default:%40Vns123456@videv.cloud:6379/1"
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=2.5,           # timeout đọc/ghi
    socket_connect_timeout=2.0,   # timeout connect
    health_check_interval=30,     # ping định kỳ 30s
    max_connections=30,            # Mỗi container chỉ tối đa 3 socket tới Redis
    timeout=1.0,                  # Khi pool bận, chờ tối đa 1s để lấy connection (không drop)
)
r = redis.Redis(connection_pool=POOL)

# Danh sách mã
symbol_list = total_list
SCHEMA = "history_tradingview"

# Hàm lấy dữ liệu từ PostgreSQL và lưu vào Redis
def get_data_and_cache(symbol):
    query = text(f"""
        SELECT "time", "symbol", "open", "high", "low", "close", "volume"
        FROM "{SCHEMA}"."{symbol}_1D"
        WHERE "time"::date != CURRENT_DATE
        ORDER BY time DESC
        LIMIT 200
    """)
    try:
        df = pd.read_sql(query, con=engine)
        if not df.empty:
            df = df.sort_values('time', ascending=True).reset_index(drop=True)
            redis_list = [
                json.dumps({
                    "time": row["time"].strftime("%Y-%m-%d %H:%M:%S"),
                    "symbol": row["symbol"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"]
                }) for _, row in df.iterrows()
            ]
            redis_key = f"{SCHEMA}:{symbol}"
            r.delete(redis_key)
            r.rpush(redis_key, *redis_list)
            print(f"✅ Đã lưu Redis: {symbol}")
            return symbol
        else:
            print(f"⚠️ Không có dữ liệu: {symbol}")
    except Exception as e:
        print(f"Lỗi {symbol}:{e}")
    return None

# Hàm chạy đa luồng
def run_multithreaded_cache():
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(get_data_and_cache, symbol) for symbol in symbol_list]

        for future in as_completed(futures):
            _ = future.result()  # Có thể xử lý kết quả nếu cần

# Chạy chính
if __name__ == "__main__":
    print(f"🚀 Bắt đầu lưu dữ liệu Redis cho {len(symbol_list)} mã...")
    run_multithreaded_cache()
    print("✅ Hoàn tất lưu Redis.")

