import json
import threading
import time
import os
import importlib
import logging
import queue

import redis

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

from ssi_sdk import Auth, Stream, Config
from ssi_sdk.enums import Timeframe

redis_url=os.getenv("redis_url")
# db_url=os.getenv("postgres_url")
db_url=os.getenv("postgres_url_test")

# ==================================================
# LOGGING
# ==================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


# ==================================================
# TIMEZONE
# ==================================================
VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# ==================================================
# LOAD SYMBOL LIST FROM ENV
# ==================================================
SYMBOL_MODULE = os.getenv("SYMBOL_MODULE", "List.exchange")
SYMBOL_NAME = os.getenv("SYMBOL_NAME")

if not SYMBOL_NAME:
    raise RuntimeError(
        "SYMBOL_NAME chưa được set (vd: HNX1, HOSE3, UPCOM4)"
    )

try:
    module = importlib.import_module(SYMBOL_MODULE)
    SYMBOLS = getattr(module, SYMBOL_NAME)

except Exception as e:
    raise RuntimeError(
        f"Không load được list {SYMBOL_MODULE}.{SYMBOL_NAME}: {e}"
    )

if not isinstance(SYMBOLS, (list, tuple)) or not SYMBOLS:
    raise RuntimeError(
        f"SYMBOL_NAME {SYMBOL_NAME} không hợp lệ hoặc rỗng"
    )

logging.info(
    "Loaded %d symbols from %s.%s",
    len(SYMBOLS),
    SYMBOL_MODULE,
    SYMBOL_NAME,
)


# ==================================================
# DERIVE EXCHANGE NAME
# ==================================================
if SYMBOL_NAME.startswith("HNX"):
    EXCHANGE = "HNX"

elif SYMBOL_NAME.startswith("HOSE"):
    EXCHANGE = "HOSE"

elif SYMBOL_NAME.startswith("UPCOM"):
    EXCHANGE = "UPCOM"

else:
    EXCHANGE = "DERIVATIVES"


# ==================================================
# CONFIG
# ==================================================
SCHEMA = os.getenv(
    "DB_SCHEMA",
    "ohlcv"
)

REDIS_CHANNEL = os.getenv(
    "REDIS_CHANNEL",
    "ssi_ohlcv_test_1"
)

LATEST_SSI_KEY_PREFIX = os.getenv(
    "LATEST_SSI_KEY_PREFIX",
    "latest_ssi_streaming_message_1"
)


# ==================================================
# SSI V3 CONFIG
# ==================================================

SSI_CONFIG = Config(
    client_id=os.getenv("client_id"),
    api_key=os.getenv("api_key"),
    api_secret=os.getenv("api_secret"),
    private_key=os.getenv("private_key"),
)


# ==================================================
# POSTGRES
# ==================================================
engine = create_engine(
    db_url,
    pool_size=5,
    max_overflow=5,
    pool_timeout=20,
    pool_recycle=1800,
    pool_pre_ping=True,
)


# ==================================================
# REDIS CONNECTION
# ==================================================
redis_pool = None
redis_client = None
redis_lock = threading.Lock()


def create_redis():
    global redis_pool

    if redis_pool is not None:
        try:
            redis_pool.disconnect()
        except Exception:
            pass

    redis_pool = redis.BlockingConnectionPool.from_url(
        redis_url,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
        max_connections=30,
        timeout=1.0,
    )

    return redis.Redis(
        connection_pool=redis_pool
    )


def reconnect_redis():
    global redis_client

    with redis_lock:
        logging.warning(
            "[REDIS] Reconnecting..."
        )

        try:
            redis_client = create_redis()
            redis_client.ping()

            logging.info(
                "[REDIS] Reconnect OK"
            )

            return True

        except Exception as e:
            logging.error(
                "[REDIS] Reconnect failed: %s",
                e
            )

            return False


def publish_redis(
    payload,
    channel=REDIS_CHANNEL,
):
    data = json.dumps(
        payload,
        ensure_ascii=False
    )

    for attempt in range(1, 4):

        try:
            redis_client.publish(
                channel,
                data
            )

            return True

        except Exception as e:
            logging.warning(
                "[REDIS PUBLISH] Failed | "
                "channel=%s | attempt=%d/3 | error=%s",
                channel,
                attempt,
                e,
            )

            reconnect_redis()

            if attempt < 3:
                time.sleep(1)

    logging.error(
        "[REDIS PUBLISH] Give up | restarting process..."
    )

    os._exit(1)


redis_client = create_redis()

try:
    redis_client.ping()

    logging.info(
        "Connected Redis | channel=%s",
        REDIS_CHANNEL
    )

except Exception as e:
    logging.error(
        "Initial Redis connection failed: %s",
        e
    )

    if not reconnect_redis():
        raise


# ==================================================
# SAVE LATEST SSI MESSAGE TO REDIS
# ==================================================
def save_latest_ssi_message(payload):

    symbol = str(
        payload.get("symbol") or ""
    ).strip().upper()

    if not symbol:
        logging.warning(
            "[REDIS SET] Missing symbol"
        )

        return False

    key = (
        f"{LATEST_SSI_KEY_PREFIX}:"
        f"{symbol}"
    )

    value = json.dumps(
        payload,
        ensure_ascii=False
    )

    for attempt in range(1, 4):

        try:
            redis_client.set(
                key,
                value
            )

            return True

        except Exception as e:
            logging.warning(
                "[REDIS SET] Failed | "
                "key=%s | attempt=%d/3 | error=%s",
                key,
                attempt,
                e,
            )

            reconnect_redis()

            if attempt < 3:
                time.sleep(1)

    logging.error(
        "[REDIS SET] Give up | "
        "key=%s | restarting process...",
        key,
    )

    os._exit(1)


# ==================================================
# UPSERT 1
# ==================================================
def upsert_1(symbol, data):

    time_vn = data["time"]

    table = (
        f'"{SCHEMA}".'
        f'"{symbol.upper()}_1"'
    )

    with engine.begin() as conn:

        conn.execute(
            text(
                f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";'
            )
        )

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    time TIMESTAMPTZ PRIMARY KEY,
                    open DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    volume BIGINT
                );
                """
            )
        )

        conn.execute(
            text(
                f"""
                INSERT INTO {table}
                (
                    symbol,
                    time,
                    open,
                    close,
                    high,
                    low,
                    volume
                )

                VALUES
                (
                    :symbol,
                    :time,
                    :open,
                    :close,
                    :high,
                    :low,
                    :volume
                )

                ON CONFLICT (time)

                DO UPDATE SET
                    open=EXCLUDED.open,
                    close=EXCLUDED.close,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    volume=EXCLUDED.volume;
                """
            ),
            {
                "symbol":
                    symbol.upper(),

                "time":
                    time_vn,

                "open":
                    float(data["open"]),

                "close":
                    float(data["close"]),

                "high":
                    float(data["high"]),

                "low":
                    float(data["low"]),

                "volume":
                    int(data["volume"]),
            }
        )


# ==================================================
# DATABASE WORKER
# ==================================================
db_queue = queue.Queue(
    maxsize=10_000
)


def db_worker():

    while True:

        symbol, data = db_queue.get()

        try:
            upsert_1(
                symbol,
                data
            )

        except Exception as e:
            logging.error(
                "[DB err] %s: %s",
                symbol,
                e
            )

        finally:
            db_queue.task_done()


threading.Thread(
    target=db_worker,
    daemon=True,
    name="db-worker",
).start()


# ==================================================
# NORMALIZE SSI V3 INTERVAL MESSAGE
# ==================================================
def normalize_ssi_message(msg):

    time_vn = datetime.strptime(
        msg.interval_time,
        "%Y/%m/%d %H:%M:%S"
    ).replace(
        tzinfo=VN_TZ
    )

    trading_time_vn = datetime.strptime(
        msg.trading_time,
        "%Y/%m/%d %H:%M:%S"
    ).replace(
        tzinfo=VN_TZ
    )

    return {
        "symbol":
            msg.symbol.upper(),

        "time":
            time_vn,

        "trading_time":
            trading_time_vn,

        "open":
            float(msg.open),

        "close":
            float(msg.close),

        "high":
            float(msg.high),

        "low":
            float(msg.low),

        "volume":
            int(msg.volume),
    }


# ==================================================
# SSI V3 CALLBACK
# ==================================================
def on_data(msg):

    try:

        # ------------------------------------------
        # SUBSCRIBE ACK / ERROR
        # ------------------------------------------
        if isinstance(msg, dict):

            logging.info(
                "[SSI] %s",
                msg
            )

            return


        # ------------------------------------------
        # KHÔNG PHẢI INTERVAL MESSAGE
        # ------------------------------------------
        if not hasattr(
            msg,
            "interval_time"
        ):
            return


        # ------------------------------------------
        # NORMALIZE
        # ------------------------------------------
        data = normalize_ssi_message(
            msg
        )

        symbol = data["symbol"]


        # ------------------------------------------
        # DATABASE QUEUE
        # ------------------------------------------
        try:
            db_queue.put_nowait(
                (
                    symbol,
                    data
                )
            )

        except queue.Full:
            logging.warning(
                "[db-queue-full] dropped %s",
                symbol
            )


        # ------------------------------------------
        # REDIS PAYLOAD
        # ------------------------------------------
        payload = {
            "function":
                "ohlcv_1",

            "symbol":
                symbol,

            "time":
                data["time"].strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "open":
                data["open"],

            "close":
                data["close"],

            "high":
                data["high"],

            "low":
                data["low"],

            "volume":
                data["volume"],

            "exchange":
                EXCHANGE,
        }


        # ------------------------------------------
        # REDIS PUBSUB
        # ------------------------------------------
        publish_redis(
            payload,
            REDIS_CHANNEL,
        )


        # ------------------------------------------
        # LATEST MESSAGE
        # ------------------------------------------
        save_latest_ssi_message(
            payload
        )


    except Exception:
        logging.exception(
            "[SSI] on_data error"
        )


# ==================================================
# HEARTBEAT
# ==================================================
def on_heartbeat(msg):
    pass


# ==================================================
# SSI V3 STREAM
# ==================================================
def stream_worker():

    while True:

        try:

            logging.info(
                "[SSI] Authenticating..."
            )

            with Auth(
                SSI_CONFIG
            ) as auth:

                auth.authenticate()

                logging.info(
                    "[SSI] Authentication OK"
                )

                with Stream(
                    auth
                ) as stream:

                    stream.streaming.on_data = (
                        on_data
                    )

                    stream.streaming.on_heartbeat = (
                        on_heartbeat
                    )

                    logging.info(
                        "[SSI] Connecting WebSocket..."
                    )

                    stream.streaming.connect()

                    logging.info(
                        "[SSI] WebSocket connected"
                    )


                    # ==================================
                    # SUBSCRIBE TOÀN BỘ SYMBOL 1M
                    # ==================================
                    stream.streaming.subscribe_symbol_ohlcv(
                        SYMBOLS,
                        interval=Timeframe.MINUTE_1
                    )

                    logging.info(
                        "[SSI] Subscribed %d symbols @ 1m",
                        len(SYMBOLS)
                    )


                    # BLOCK
                    stream.streaming.wait()


        except KeyboardInterrupt:

            logging.info(
                "[SSI] Stopped by user"
            )

            return


        except Exception:

            logging.exception(
                "[SSI] Stream crashed"
            )

            logging.warning(
                "[SSI] Reconnect after 3 seconds..."
            )

            time.sleep(3)


# ==================================================
# MAIN
# ==================================================
if __name__ == "__main__":

    logging.info(
        "SSI V3 OHLCV 1M | "
        "symbols=%d | exchange=%s",
        len(SYMBOLS),
        EXCHANGE
    )

    stream_worker()