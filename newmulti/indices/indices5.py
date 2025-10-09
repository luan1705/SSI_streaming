# producer_x_keep_result.py
import os, json, time, logging, signal, sys, redis
from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# ====== IMPORTS THEO DỰ ÁN CỦA BẠN ======
from List import confighao as config
from List.upsert import upsert_mi
from List.indice import indice5
# =========================================

# ---------- Cấu hình qua ENV ----------
REDIS_URL   = "redis://default:%40Vns123456@localhost:6379/1"
STREAM_CODE = "MI:" + "-".join(indice5)
CHANNEL = "indices_5"
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

def on_message_MI(message):
    try:
        data = json.loads(message.get("Content","{}"))
        symbol=data['IndexId']
        symbol = 'UPCOMINDEX' if symbol == 'HNXUpcomIndex' else symbol
        symbol = 'HNXINDEX' if symbol == 'HNXIndex' else symbol

        result = {
            'function': 'indices',
            'content': {
                'symbol': symbol,
                'point': data['IndexValue'],
                'change': data['Change'],
                'ratioChange': data['RatioChange'],
                'totalVol': data['AllQty'],
                'totalVal': data['AllValue'],
                'advancersDecliners': [
                    data['Advances']+data['Ceilings'],
                    data['NoChanges'],
                    data['Declines']+data['Floors']
                ]
            }
        }
        # Publish result sang Redis để Hub gom về 1 WS port
        publish(result)

        # save DB
        c = result["content"]
        # Chuẩn hoá advancersDecliners về đúng 3 phần tử
        raw = c.get("advancersDecliners")
        if isinstance(raw, dict):
            seq = [raw.get("Advances"), raw.get("NoChanges"), raw.get("Declines")]
        else:
            seq = list(raw) if raw is not None else []

        adv, nc, dec = (seq + [None, None, None])[:3]

        row = {
            "symbol":       c["symbol"],
            "point":        c["point"],
            "change":       c["change"],
            "ratioChange":  c["ratioChange"],
            "totalVol":  c["totalVol"],
            "totalVal":   c["totalVal"],
            "advancers":    adv,
            "noChange":     nc,
            "decliners":    dec,
        }
        upsert_mi(row)
    except Exception:
        logging.exception("MI message error")

def on_error(err):
    logging.error(f"MI stream error: {err}")
    raise Exception(err)

def on_close():
    logging.warning("Stream closed, will reconnect...")

def main():
    logging.info("Producer MI | stream=%s | publish=%s", STREAM_CODE, CHANNEL)

    # graceful shutdown
    signal.signal(signal.SIGINT,  lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    connectssi=MarketDataClient(config)
    while True:
        try:
            mm = MarketDataStream(config,connectssi)
            mm.start(on_message_MI, on_error, STREAM_CODE, on_close)
        except Exception as e:
            logging.error("Stream crashed: %s", e)

        logging.info("Disconnected (goodbye). Reconnect in 5s...")
        time.sleep(2)

if __name__ == "__main__":
    main()