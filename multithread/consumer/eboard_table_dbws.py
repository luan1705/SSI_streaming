import orjson
import asyncio
import logging
import re
from collections import defaultdict

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_x_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer X Auto PARALLEL + WebSocket + Postgres")
clients = {"X": set()}

# LOCK theo symbol + cache (giống MI)
symbol_locks = defaultdict(asyncio.Lock)
last_saved_data = {}

async def broadcast(channel: str, data: dict):
    """Gửi dữ liệu cho tất cả WebSocket client."""
    dead = []
    payload = orjson.dumps(data).decode()
    for ws in list(clients[channel]):
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients[channel].discard(ws)

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

# -------------------- Helpers & DB save --------------------
def _take(lst, idx, default=None):
    """Safe array access"""
    try:
        return lst[idx] if lst and idx < len(lst) else default
    except Exception:
        return default

def _norm_side(side):
    """Chuẩn hoá block 'buy'/'sell'"""
    if not isinstance(side, dict):
        return [], []
    prices = side.get("price") or []
    vols = side.get("vol") or []
    return prices, vols

def save_x(result: dict):
    """Lưu data vào Postgres với validation (đồng bộ)"""
    try:
        # Validation
        if "content" not in result:
            logging.error(f"❌ Missing 'content' key: {result}")
            return

        c = result["content"]
        sym = c.get("symbol")
        if not sym:
            logging.error(f"❌ Missing symbol in content: {c}")
            return

        # Parse indices
        indices = c.get("indices")
        if isinstance(indices, list):
            indices = "|".join(map(str, indices))
        elif indices is not None:
            indices = str(indices)

        # Parse buy/sell
        bp, bv = _norm_side(c.get("buy"))
        sp, sv = _norm_side(c.get("sell"))
        m = c.get("match") or {}

        # Build row
        row = {
            "symbol": sym,
            "exchange": c.get("exchange"),
            "indices": indices,
            "ceiling": float(c["ceiling"]) if c.get("ceiling") is not None else None,
            "floor": float(c["floor"]) if c.get("floor") is not None else None,
            "refPrice": float(c["refPrice"]) if c.get("refPrice") is not None else None,
            "buyPrice3": float(_take(bp, 2)) if _take(bp, 2) is not None else None,
            "buyVol3": float(_take(bv, 2)) if _take(bv, 2) is not None else None,
            "buyPrice2": float(_take(bp, 1)) if _take(bp, 1) is not None else None,
            "buyVol2": float(_take(bv, 1)) if _take(bv, 1) is not None else None,
            "buyPrice1": float(_take(bp, 0)) if _take(bp, 0) is not None else None,
            "buyVol1": float(_take(bv, 0)) if _take(bv, 0) is not None else None,
            "matchPrice": float(m["price"]) if m.get("price") is not None else None,
            "matchVol": float(m["vol"]) if m.get("vol") is not None else None,
            "matchChange": float(m["change"]) if m.get("change") is not None else None,
            "matchRatioChange": float(m["ratioChange"]) if m.get("ratioChange") is not None else None,
            "sellPrice1": float(_take(sp, 0)) if _take(sp, 0) is not None else None,
            "sellVol1": float(_take(sv, 0)) if _take(sv, 0) is not None else None,
            "sellPrice2": float(_take(sp, 1)) if _take(sp, 1) is not None else None,
            "sellVol2": float(_take(sv, 1)) if _take(sv, 1) is not None else None,
            "sellPrice3": float(_take(sp, 2)) if _take(sp, 2) is not None else None,
            "sellVol3": float(_take(sv, 2)) if _take(sv, 2) is not None else None,
            "totalVol": float(c["totalVol"]) if c.get("totalVol") is not None else None,
            "totalVal": float(c["totalVal"]) if c.get("totalVal") is not None else None,
            "high": float(c["high"]) if c.get("high") is not None else None,
            "low": float(c["low"]) if c.get("low") is not None else None,
            "open": float(c["open"]) if c.get("open") is not None else None,
            "close": float(c["close"]) if c.get("close") is not None else None,
        }

        # Log trước khi save
        mp = row.get("matchPrice", "N/A")
        logging.info(f"💾 Saving {row['symbol']} | matchPrice={mp}")

        # Cache
        last_saved_data[row["symbol"]] = row.copy()

        # Upsert
        with engine.begin() as conn:
            stmt = pg_insert(x_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)

        logging.info(f"✅ Upserted {row['symbol']}")

    except ValueError as e:
        logging.error(f"❌ Type conversion error: {e} | data: {result}")
    except Exception as e:
        logging.error(f"❌ DB save error: {e} | data={result}", exc_info=True)

async def save_x_async(result: dict):
    """Wrapper async để gọi save_x trong threadpool"""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_x, result)

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
    """Consumer riêng cho 1 topic - CHỈ LẤY MESSAGE MỚI LIÊN TỤC"""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id=f"eboard_x_{topic}",
        auto_offset_reset="latest",  # CHỈ lấy message MỚI
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
                match_price = result.get("content", {}).get("match", {}).get("price", "N/A")

                logging.info(f"📥 {topic}: {symbol} | matchPrice={match_price}")

                # Broadcast to WebSocket (non-blocking)
                asyncio.create_task(broadcast("X", result))

                # Save to DB với LOCK theo symbol (đảm bảo thứ tự)
                async with symbol_locks[symbol]:
                    await save_x_async(result)

            except Exception as e:
                logging.error(f"❌ Error processing {topic}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"❌ Consumer {topic} failed: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logging.warning(f"⚠️ Consumer stopped for topic: {topic}")

async def start_all_consumers():
    """Tạo consumer riêng cho mỗi topic (parallel)"""
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

# -------------------- Debug endpoints --------------------
@app.get("/health")
async def health():
    return {
        "status": "running",
        "websocket_clients": len(clients["X"]),
        "message": "All consumers running in parallel"
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