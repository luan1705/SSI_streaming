import orjson
import asyncio
import logging
import re
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, DECIMAL
from sqlalchemy.dialects.postgresql import insert as pg_insert
from decimal import Decimal
from collections import defaultdict

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("consumer_mi_auto.log"), logging.StreamHandler()],
)

# -------------------- FastAPI --------------------
app = FastAPI(title="Kafka Consumer MI Auto PARALLEL + WebSocket + Postgres")
clients = {"MI": set()}

# LOCK để tránh race condition khi save DB
symbol_locks = defaultdict(asyncio.Lock)
last_saved_data = {}  # Cache message cuối cùng đã save

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
async def websocket_mi(ws: WebSocket):
    await websocket_endpoint(ws, "MI")

# -------------------- Postgres setup --------------------
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=40
)
metadata = MetaData()

mi_table = Table(
    "indices", metadata,
    Column("symbol", String, primary_key=True),
    Column("point", Float),
    Column("change", Float),
    Column("ratioChange", Float),
    Column("totalVol", Float),
    Column("totalVal", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    schema="history_data"
)

metadata.create_all(engine)

async def save_mi_async(result):
    """Wrapper async để dùng lock"""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, save_mi, result)

def save_mi(result):
    """Lưu data vào Postgres với validation"""
    try:
        # Validation
        if "content" not in result:
            logging.error(f"❌ Missing 'content' key: {result}")
            return
        
        c = result["content"]
        
        # Validation các field bắt buộc
        required_fields = ["symbol", "point", "change", "ratioChange", "totalVol", "totalVal"]
        for field in required_fields:
            if field not in c:
                logging.error(f"❌ Missing field '{field}' in content: {c}")
                return
        
        # Parse advancersDecliners
        raw = c.get("advancersDecliners", [])
        if not isinstance(raw, list) or len(raw) != 3:
            logging.warning(f"⚠️ Invalid advancersDecliners format: {raw}, using [None, None, None]")
            adv, nc, dec = None, None, None
        else:
            adv, nc, dec = raw[0], raw[1], raw[2]
        
        # Convert to proper types
        row = {
            "symbol": str(c["symbol"]),
            "point": float(c["point"]) if c["point"] is not None else None,
            "change": float(c["change"]) if c["change"] is not None else None,
            "ratioChange": float(c["ratioChange"]) if c["ratioChange"] is not None else None,
            "totalVol": float(c["totalVol"]) if c["totalVol"] is not None else None,
            "totalVal": float(c["totalVal"]) if c["totalVal"] is not None else None,
            "advancers": int(adv) if adv is not None else None,
            "noChange": int(nc) if nc is not None else None,
            "decliners": int(dec) if dec is not None else None,
        }
        
        # Log trước khi save
        logging.info(f"💾 Saving to DB: {row['symbol']} | point={row['point']:.2f} | change={row['change']:.2f}")
        
        # Lưu vào cache để so sánh
        symbol_key = row['symbol']
        last_saved_data[symbol_key] = row.copy()
        
        # Upsert
        with engine.begin() as conn:
            stmt = pg_insert(mi_table).values([row])
            update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
            stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
            conn.execute(stmt)
        
        logging.info(f"✅ Upserted {row['symbol']}")
        
    except ValueError as e:
        logging.error(f"❌ Type conversion error: {e} | data: {result}")
    except Exception as e:
        logging.error(f"❌ DB save error: {e} | data: {result}", exc_info=True)

# -------------------- Kafka consumer (1 consumer per topic) --------------------
KAFKA_BROKER = "localhost:9092"
TOPIC_PATTERN = r"^indice_.*"

async def get_matching_topics():
    """Lấy tất cả topics match pattern indice_*"""
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
    """Consumer riêng cho 1 topic cụ thể - CHỈ LẤY MESSAGE MỚI LIÊN TỤC"""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: orjson.loads(m.decode()),
        group_id=f"eboard_mi_{topic}",  # Group ID riêng cho mỗi topic
        auto_offset_reset="latest",  # CHỈ lấy message MỚI từ bây giờ trở đi
        enable_auto_commit=True,
        max_poll_records=10,  # Lấy max 10 message mỗi lần poll
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )
    
    await consumer.start()
    logging.info(f"🚀 Consumer started for topic: {topic} (realtime mode)")
    
    try:
        async for msg in consumer:
            try:
                result = msg.value
                symbol = result.get('content', {}).get('symbol', 'N/A')
                point = result.get('content', {}).get('point', 0)
                
                logging.info(f"📥 {topic}: {symbol} | point={point}")
                
                # Broadcast to WebSocket (non-blocking)
                asyncio.create_task(broadcast("MI", result))
                
                # Save to DB với LOCK theo symbol để tránh race condition
                async with symbol_locks[symbol]:
                    await save_mi_async(result)
                
            except Exception as e:
                logging.error(f"❌ Error processing {topic}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"❌ Consumer {topic} failed: {e}", exc_info=True)
    finally:
        await consumer.stop()
        logging.warning(f"⚠️ Consumer stopped for topic: {topic}")

async def start_all_consumers():
    """Tạo consumer riêng cho mỗi topic"""
    topics = await get_matching_topics()
    
    if not topics:
        logging.warning("⚠️ No matching topics found!")
        return
    
    # Tạo task riêng cho mỗi topic
    tasks = [
        asyncio.create_task(consume_single_topic(topic))
        for topic in topics
    ]
    
    logging.info(f"✅ Started {len(tasks)} parallel consumers")
    
    # Chờ tất cả consumers (chạy mãi mãi)
    await asyncio.gather(*tasks, return_exceptions=True)

# -------------------- FastAPI startup --------------------
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(start_all_consumers())
    logging.info("🚀 Kafka parallel consumers started!")

@app.get("/health")
async def health():
    return {
        "status": "running",
        "websocket_clients": len(clients["MI"]),
        "message": "All consumers running in parallel"
    }

@app.get("/db/indices")
async def get_all_indices():
    """Debug endpoint: xem tất cả data trong DB"""
    with engine.connect() as conn:
        result = conn.execute(mi_table.select())
        rows = [dict(row._mapping) for row in result]
    return {"total": len(rows), "data": rows}

@app.get("/db/indices/{symbol}")
async def get_indice(symbol: str):
    """Debug endpoint: xem 1 symbol cụ thể"""
    with engine.connect() as conn:
        result = conn.execute(
            mi_table.select().where(mi_table.c.symbol == symbol)
        )
        row = result.fetchone()
    if row:
        return dict(row._mapping)
    return {"error": f"Symbol {symbol} not found"}