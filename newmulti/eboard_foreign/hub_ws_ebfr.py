# hub_ws.py
import json, asyncio, logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import redis.asyncio as redis  # pip install redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
app = FastAPI(title="Unified WS Hub")
clients: set[WebSocket] = set()

# Redis trong LAN/Docker network
r = redis.from_url("redis://default:%40Vns123456@videv.cloud:6379/1", decode_responses=True)

async def broadcast(payload: dict | str):
    txt = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    dead = []
    for ws in list(clients):
        try:
            await ws.send_text(txt)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)

@app.websocket("/")
async def ws_all(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # giữ kết nối sống
    except WebSocketDisconnect:
        logging.info("Client disconnected %s", websocket.client)
    finally:
        clients.discard(websocket)

async def redis_listener():
    pubsub = r.pubsub()
    await pubsub.psubscribe("ebfr_*")  # nhận tất cả kênh ws:1..ws:12
    logging.info("Hub subscribed to Redis pattern: ebfr_*")
    async for msg in pubsub.listen():
        if msg.get("type") == "pmessage":
            ch, raw = msg.get("channel"), msg.get("data")
            try:
                obj = json.loads(raw)
            except Exception:
                obj = {"source": ch, "data": raw}
            if isinstance(obj, dict) and "source" not in obj:
                obj["source"] = ch
            await broadcast(obj)

@app.on_event("startup")
async def on_start():
    asyncio.create_task(redis_listener())
    logging.info("Unified WS Hub started")

# Chạy: uvicorn hub_ws:app --host 0.0.0.0 --port 9002
