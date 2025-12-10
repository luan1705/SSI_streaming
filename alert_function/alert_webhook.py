import redis
import os
import json
import requests

WEBHOOK_URL = "https://n8n.videv.cloud/webhook/redis_alert"

REDIS_URL = os.getenv("REDIS_URL", "redis://default:%40Vns123456@videv.cloud:6379/1")
POOL = redis.BlockingConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_timeout=60,
    socket_connect_timeout=5,
    health_check_interval=30,
    max_connections=3,
    timeout=1.0,
)
r = redis.Redis(connection_pool=POOL)

# ✅ Map mã tín hiệu -> câu tiếng Việt
EVENT_LABELS = {
    "ma10_cross_up": "Giá cắt lên đường <b>MA10</b>",
    "ma20_cross_up": "Giá cắt lên đường <b>MA20</b>",
    "ma50_cross_up": "Giá cắt lên đường <b>MA50</b>",

    "ma10_cross_down": "Giá cắt xuống đường <b>MA10</b>",
    "ma20_cross_down": "Giá cắt xuống đường <b>MA20</b>",
    "ma50_cross_down": "Giá cắt xuống đường <b>MA50</b>",

    "macd_cross_up": "<b>MACD</b> cắt lên đường tín hiệu",
    "macd_cross_down": "<b>MACD</b> cắt xuống đường tín hiệu",

    "bb_upper_cross_up": "Giá cắt lên ra ngoài biên trên <b>Bollinger Bands</b>",
    "bb_upper_cross_down": "Giá cắt xuống từ ngoài biên trên <b>Bollinger Bands</b>",
    "bb_lower_cross_down": "Giá cắt xuống ra ngoài biên dưới <b>Bollinger Bands</b>",
    "bb_lower_cross_up": "Giá cắt lên từ ngoài biên dưới <b>Bollinger Bands</b>",
    # sau này có thêm event thì cứ bổ sung vào đây
}

def main():
    pubsub = r.pubsub()
    # Ở đây chỉ cần subscribe thường, không cần psubscribe
    pubsub.subscribe("alert_function")
    print("STATUS CONSUMER: listening on channel: alert_function ...")

    while True:
        try:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)

            # Không có gì thì bỏ qua, loop tiếp
            if msg is None:
                continue

            # Chỉ xử lý message bình thường
            if msg["type"] != "message":
                continue

            data_str = msg["data"]          # do decode_responses=True nên data_str là string
            try:
                raw = json.loads(data_str)
            except json.JSONDecodeError as e:
                print("❌ Không parse được JSON:", e, "data:", data_str)
                continue

            # Chỉ lấy alert_trigger, bỏ qua alert_status nếu có
            if raw.get("function") != "alert_trigger":
                continue

            # Lấy dữ liệu từ message
            content = raw.get("content", {})
            symbol = content.get("symbol")
            time_str = content.get("time")
            events = content.get("event", [])

            # 🔁 Build message nhiều dòng:
            # Mã ACB:
            # _ Đường MA20 cắt lên giá lúc ...
            # _ Đường MA50 cắt xuống giá lúc ...
            lines = [f"Mã <b>{symbol}</b>:"]
            for ev in events:
                label = EVENT_LABELS.get(ev, ev)
                lines.append(f"_ {label} lúc {time_str}")

            text = "\n".join(lines)

            http_payload = {
                "text": text,        # n8n đang dùng body.text
                "symbol": symbol,
                "time": time_str,
                "events": events,
                "raw": raw,          # giữ full message để sau này cần thì xài
            }

            try:
                resp = requests.post(WEBHOOK_URL, json=http_payload, timeout=5)
                print("➜ Sent to webhook:", resp.status_code, text)
            except Exception as e:
                print("❌ Error sending to webhook:", e)

        except Exception as e:
            print(f"❌ Lỗi alert function (vòng while): {e}")


if __name__ == "__main__":
    main()
