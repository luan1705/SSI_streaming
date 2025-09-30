# file: kafka_consumer_r_auto.py

import orjson
import asyncio
import logging
import re
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_r_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer R Auto + WebSocket + Postgres")
clients = {"R": set()}


async def broadcast(channel, data: dict):
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
async def websocket_r(ws: WebSocket):
    await websocket_endpoint(ws, "R")


# -------------------- Postgres setup --------------------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
)
metadata = MetaData()

r_table = Table(
    "eboard_foreign",
    metadata,
    Column("symbol", String, primary_key=True),
    Column("buyVol", Float),
    Column("sellVol", Float),
    Column("room", Float),
    Column("buyVal", Float),
    Column("sellVal", Float),
    schema="history_data",
)

metadata.create_all(engine)


def save_r(result):
    try:
        c = result["content"]
        row = {
            "symbol": c["symbol"],
            "buyVol": c["buyVol"],
            "sellVol": c["sellVol"],
            "room": c["room"],
            "buyVal": c["buyVal"],
            "sellVal": c["sellVal"],
        }

        with engine.begin() as conn:
            stmt = pg_insert(r_table).values([row])
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
TOPIC_PREFIX = "^eboard_foreign_.*"


async def consume():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id="eboard_r_group",
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
                await broadcast("R", result)

                # lưu vào DB trong threadpool để không block event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, save_r, result)

            except Exception as e:
                logging.error(f"❌ Consumer error: {e}")

    finally:
        await consumer.stop()


# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(consume())
    logging.info("🚀 Kafka auto-consumer R started")
