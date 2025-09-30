# file: kafka_consumer_x_auto.py

import orjson
import asyncio
import logging
import re
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert
from collections import defaultdict

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_x_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer X Auto PARALLEL + WebSocket + Postgres")
clients = {"X": set()}

symbol_locks = defaultdict(asyncio.Lock)  # lock theo symbol để tránh race condition
last_saved_data = {}  # cache dữ liệu cuối cùng đã save


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
    pool_size=20,
    max_overflow=40,
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
    """Lưu data vào Postgres với validation"""
    try:
        if "content" not in result:
            logging.error(f"❌ Missing 'content': {result}")
            return
        c = result["content"]

        indices = c.get("indices")
        if isinstance(indices, list):
            indices = "|".join(indices)

        bp, bv = c["buy"]["price"], c["buy"]["vol"]
        sp, sv = c["sell"]["price"], c["sell"]["vol"]
        m = c["match"]

        row = {
            "symbol": c["symbol"],
            "exchange": c.get("exchange"),
            "indices": indices,
            "ceiling": c.get("ceiling"),
            "floor": c.get("floor"),
            "refPrice": c.get("refPrice"),
            "buyPrice1": bp[0],
            "buyVol1": bv[0],
            "buyPrice2": bp[1],
            "buyVol2": bv[1],
            "buyPrice3": bp[2],
            "buyVol3": bv[2],
            "matchPrice": m.get("price"),
            "matchVol": m.get("vol"),
            "matchChange": m.get("change"),
            "matchRatioChange": m.get("ratioChange"),
            "sellPrice1": sp[0],
            "sellVol1": sv[0],
            "sellPrice2": sp[1],
            "sellVol2": sv[1],
            "sellPrice3": sp[2],
            "sellVol3": sv[2],
            "totalVol": c.get("totalVol"),
            "totalVal": c.get("totalVal"),
            "high": c.get("high"),
            "low": c.get("low"),
            "open": c.get("open"),
            "close": c.get("close"),
        }

        logging.info(f"💾 Saving {row['symbol']} | matchPrice={row['matchPrice']}")
        last_saved_data[row["symbol"]] = row.copy()

        with engine.begin() as conn:
            stmt = pg_insert(x_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)

        logging.info(f"✅ Upserted {row['symbol']}")
    except Exception as e:
        logging.error(f"❌ DB save error: {e} | data={result}", exc_info=True)


# -------------------- Kafka consumer --------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_PATTERN = r"^eboard_table_.*"


async def get_matching_topics():
    """Lấy tất cả topics match pattern eboard_table_*"""
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    await admin.start()
    try:
        metadata = await admin.list_topics()
        pattern = re.compile(TOPIC_PATTERN)
        matching = [t for t in metadata if pattern.match(t)]
        logging.info(f"🔍 Found {len(matching)} matching topics: {matching}")
        return matching
    finally:
        await admin.close()


async def consume_single_topic(topic: str):
    """Consumer riêng cho 1 topic"""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id=f"eboard_x_{topic}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    await consumer.start()
    logging.info(f"🚀 Consumer started for topic: {topic}")
    try:
        async for msg in consumer:
            result = msg.value
            symbol = result.get("content", {}).get("symbol", "N/A")
            logging.info(f"📥 {topic}: {symbol}")

            # broadcast WS
            asyncio.create_task(broadcast("X", result))

            # save DB
            async with symbol_locks[symbol]:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, save_x, result)
    except Exception as e:
        logging.error(f"❌ Consumer {topic} failed: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logging.warning(f"⚠️ Consumer stopped for {topic}")


async def start_all_consumers():
    topics = await get_matching_topics()
    if not topics:
        logging.warning("⚠️ No matching topics found!")
        return
    tasks = [asyncio.create_task(consume_single_topic(t)) for t in topics]
    logging.info(f"✅ Started {len(tasks)} parallel consumers")
    await asyncio.gather(*tasks, return_exceptions=True)


# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_all_consumers())
    logging.info("🚀 Kafka parallel consumers X started!")


@app.get("/health")
async def health():
    return {
        "status": "running",
        "websocket_clients": len(clients["X"]),
        "topics_consumed": list(await get_matching_topics()),
    }


@app.get("/db/eboard")
async def get_all_rows():
    with engine.connect() as conn:
        result = conn.execute(x_table.select())
        rows = [dict(row._mapping) for row in result]
    return {"total": len(rows), "data": rows}


@app.get("/db/eboard/{symbol}")
async def get_symbol(symbol: str):
    with engine.connect() as conn:
        result = conn.execute(x_table.select().where(x_table.c.symbol == symbol))
        row = result.fetchone()
    if row:
        return dict(row._mapping)
    return {"error": f"Symbol {symbol} not found"}
