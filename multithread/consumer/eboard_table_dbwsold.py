# file: kafka_consumer_x_auto.py

import orjson
import asyncio
import logging
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert
import re

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_x_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer X Auto + WebSocket + Postgres")
clients = {"X": set()}


async def broadcast(channel, data: dict):
    """Gửi dữ liệu cho tất cả WebSocket client"""
    dead_clients = []
    for ws in list(clients[channel]):
        try:
            await ws.send_text(orjson.dumps(data).decode())
        except Exception:
            dead_clients.append(ws)
    for ws in dead_clients:
        clients[channel].remove(ws)


async def websocket_endpoint(websocket: WebSocket, channel: str):
    await websocket.accept()
    clients[channel].add(websocket)
    logging.info(f"✅ Client connected: {websocket.client} to {channel}")
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logging.info(f"❌ Client disconnected: {websocket.client} from {channel}")
    finally:
        clients[channel].discard(websocket)


@app.websocket("/")
async def websocket_x(ws: WebSocket):
    await websocket_endpoint(ws, "X")


# -------------------- Postgres setup --------------------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
)
metadata = MetaData()

x_table = Table(
    "eboard_table",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("exchange", String),
    Column("indices", String),
    Column("ceiling", Float),
    Column("floor", Float),
    Column("refPrice", Float),
    Column("buyPrice3", Float),
    Column("buyVol3", Float),
    Column("buyPrice2", Float),
    Column("buyVol2", Float),
    Column("buyPrice1", Float),
    Column("buyVol1", Float),
    Column("matchPrice", Float),
    Column("matchVol", Float),
    Column("matchChange", Float),
    Column("matchRatioChange", Float),
    Column("sellPrice1", Float),
    Column("sellVol1", Float),
    Column("sellPrice2", Float),
    Column("sellVol2", Float),
    Column("sellPrice3", Float),
    Column("sellVol3", Float),
    Column("totalVol", Float),
    Column("totalVal", Float),
    Column("high", Float),
    Column("low", Float),
    Column("open", Float),
    Column("close", Float),
    schema="history_data",
)

metadata.create_all(engine)


def save_x(result):
    try:
        c = result["content"]
        indices = c.get("indices")
        if isinstance(indices, list):
            indices = "|".join(indices)

        bp = c["buy"]["price"]
        bv = c["buy"]["vol"]
        sp = c["sell"]["price"]
        sv = c["sell"]["vol"]
        m = c["match"]

        row = {
            "symbol": c["symbol"],
            "exchange": c.get("exchange"),
            "indices": indices,
            "ceiling": c["ceiling"],
            "floor": c["floor"],
            "refPrice": c["refPrice"],
            "buyPrice1": bp[0],
            "buyVol1": bv[0],
            "buyPrice2": bp[1],
            "buyVol2": bv[1],
            "buyPrice3": bp[2],
            "buyVol3": bv[2],
            "matchPrice": m["price"],
            "matchVol": m["vol"],
            "matchChange": m["change"],
            "matchRatioChange": m["ratioChange"],
            "sellPrice1": sp[0],
            "sellVol1": sv[0],
            "sellPrice2": sp[1],
            "sellVol2": sv[1],
            "sellPrice3": sp[2],
            "sellVol3": sv[2],
            "totalVol": c["totalVol"],
            "totalVal": c["totalVal"],
            "high": c["high"],
            "low": c["low"],
            "open": c["open"],
            "close": c["close"],
        }

        with engine.begin() as conn:
            stmt = pg_insert(x_table).values([row])
            update_dict = {
                k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"
            }
            stmt = stmt.on_conflict_do_update(
                index_elements=["symbol"], set_=update_dict
            )
            conn.execute(stmt)

        logging.info("✅ Upserted %s", c["symbol"])
    except Exception as e:
        logging.error("❌ DB save error: %s", e)


# -------------------- Kafka consumer --------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_PREFIX = "^eboard_table_.*"


async def consume():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id="eboard_x_group",
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        consumer.subscribe(pattern=re.compile(TOPIC_PREFIX))
        logging.info(f"Subscribed to topic pattern: {TOPIC_PREFIX}")

        async for msg in consumer:
            try:
                result = msg.value
                logging.info(f"📥 {msg.topic}: {result}")
                await broadcast("X", result)

                # chạy save_x trong threadpool để không block event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, save_x, result)

            except Exception as e:
                logging.error(f"❌ Consumer error: {e}")

    finally:
        await consumer.stop()


# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume())
    logging.info("🚀 Kafka auto-consumer X started")
