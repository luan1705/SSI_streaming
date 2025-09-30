# file: kafka_consumer_r_auto.py

import orjson
import asyncio
import logging
import re
from collections import defaultdict
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_r_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer R Auto PARALLEL + WebSocket + Postgres")
clients = {"R": set()}

# LOCK để tránh race condition khi save DB
symbol_locks = defaultdict(asyncio.Lock)
last_saved_data = {}  # Cache message cuối cùng đã save
matched_topics = []   # Cache topics tìm được lúc startup

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
async def websocket_r(ws: WebSocket):
    await websocket_endpoint(ws, "R")

# -------------------- Postgres setup --------------------
# Thay connection string nếu cần
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=40,
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

# -------------------- Save helpers --------------------
async def save_r_async(result):
    """Wrapper async để dùng run_in_executor"""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_r, result)

def save_r(result):
    """Lưu data vào Postgres với validation và casting an toàn"""
    try:
        if "content" not in result:
            logging.error(f"❌ Missing 'content' key: {result}")
            return

        c = result["content"]

        # Kiểm tra bắt buộc
        if "symbol" not in c:
            logging.error(f"❌ Missing 'symbol' in content: {c}")
            return

        # Parse & cast
        def to_float_safe(x):
            try:
                return float(x) if x is not None else None
            except Exception:
                return None

        row = {
            "symbol": str(c.get("symbol")),
            "buyVol": to_float_safe(c.get("buyVol")),
            "sellVol": to_float_safe(c.get("sellVol")),
            "room": to_float_safe(c.get("room")),
            "buyVal": to_float_safe(c.get("buyVal")),
            "sellVal": to_float_safe(c.get("sellVal")),
        }

        # Log trước khi save
        logging.info(f"💾 Saving to DB: {row['symbol']} | buyVol={row['buyVol']} sellVol={row['sellVol']}")

        # Lưu vào cache để so sánh (nếu muốn skip duplicates sau này)
        last_saved_data[row["symbol"]] = row.copy()

        # Upsert
        with engine.begin() as conn:
            stmt = pg_insert(r_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)

        logging.info(f"✅ Upserted {row['symbol']}")

    except Exception as e:
        logging.error(f"❌ DB save error: {e} | data: {result}", exc_info=True)

# -------------------- Kafka consumer (1 consumer per topic) --------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_PATTERN = r"^eboard_foreign_.*"

async def get_matching_topics():
    """Lấy tất cả topics match pattern eboard_foreign_*"""
    admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    await admin.start()
    try:
        topics = await admin.list_topics()  # returns set of topic names
        pattern = re.compile(TOPIC_PATTERN)
        matching = [t for t in topics if pattern.match(t)]
        logging.info(f"🔍 Found {len(matching)} matching topics: {matching}")
        return matching
    finally:
        await admin.close()

async def consume_single_topic(topic: str):
    """Consumer riêng cho 1 topic cụ thể - CHỈ LẤY MESSAGE MỚI LIÊN TỤC"""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id=f"eboard_r_{topic}",     # Group ID riêng cho mỗi topic
        auto_offset_reset="latest",       # CHỈ lấy message MỚI từ bây giờ trở đi
        enable_auto_commit=True,
        max_poll_records=10,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    await consumer.start()
    logging.info(f"🚀 Consumer started for topic: {topic} (realtime mode)")

    try:
        async for msg in consumer:
            try:
                result = msg.value
                symbol = result.get("content", {}).get("symbol", "N/A")

                logging.info(f"📥 {topic}: {symbol}")

                # Broadcast to WebSocket (non-blocking)
                asyncio.create_task(broadcast("R", result))

                # Save to DB với LOCK theo symbol để tránh race condition
                async with symbol_locks[symbol]:
                    await save_r_async(result)

            except Exception as e:
                logging.error(f"❌ Error processing {topic}: {e}", exc_info=True)

    except Exception as e:
        logging.error(f"❌ Consumer {topic} failed: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logging.warning(f"⚠️ Consumer stopped for topic: {topic}")

async def start_all_consumers():
    """Tạo consumer riêng cho mỗi topic"""
    global matched_topics
    topics = await get_matching_topics()
    matched_topics = topics or []

    if not topics:
        logging.warning("⚠️ No matching topics found!")
        return

    # Tạo task riêng cho mỗi topic
    tasks = [asyncio.create_task(consume_single_topic(topic)) for topic in topics]

    logging.info(f"✅ Started {len(tasks)} parallel consumers")

    # Chờ tất cả consumers (chạy mãi mãi)
    await asyncio.gather(*tasks, return_exceptions=True)

# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_all_consumers())
    logging.info("🚀 Kafka parallel consumers R started!")

@app.get("/health")
async def health():
    return {
        "status": "running",
        "websocket_clients": len(clients["R"]),
        "topics_consumed": matched_topics,
        "message": "All consumers running in parallel"
    }

@app.get("/db/eboard_foreign")
async def get_all_rows():
    with engine.connect() as conn:
        result = conn.execute(r_table.select())
        rows = [dict(row._mapping) for row in result]
    return {"total": len(rows), "data": rows}

@app.get("/db/eboard_foreign/{symbol}")
async def get_symbol(symbol: str):
    with engine.connect() as conn:
        result = conn.execute(r_table.select().where(r_table.c.symbol == symbol))
        row = result.fetchone()
    if row:
        return dict(row._mapping)
    return {"error": f"Symbol {symbol} not found"}
