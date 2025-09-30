import orjson
import config
import asyncio
import threading
import time
from datetime import datetime, time as dtime, date
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import logging
import random
from exchange_map import exchange_map
from indices_map import indices_map

from ssi_fc_data.fc_md_stream import MarketDataStream
from ssi_fc_data.fc_md_client import MarketDataClient

# DB
import pandas as pd
from pandas import json_normalize
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError

# ==================== Optional symbol lists (để batch) ====================
try:
    from symbols_list import symbols_list as SYMBOLS_X
except Exception:
    SYMBOLS_X = None

try:
    from symbols_list import symbols_list as SYMBOLS_R
except Exception:
    SYMBOLS_R = None

try:
    from mi_indices_list import mi_indices_list as INDICES_MI
except Exception:
    INDICES_MI = None

# ==================== Logging ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("stream.log"), logging.StreamHandler()],
)

# ==================== FastAPI ====================
app = FastAPI(title="Streaming WebSocket + Multi-DB Production")

clients = {"X": set(), "R": set(), "MI": set()}
last_msg_time = {"X": None, "R": None, "MI": None}
holiday = [date(2026, 1, 1)]

# ==================== Concurrency Guards ====================
# Giới hạn số kết nối WS hoạt động cùng lúc (điều chỉnh theo tài nguyên máy)
# MAX_ACTIVE_CONN = 1000
# GLOBAL_CONN_SEM = threading.Semaphore(MAX_ACTIVE_CONN)

# Giới hạn số luồng gọi AccessToken đồng thời (giảm bão token)
TOKEN_FETCH_SEM = threading.Semaphore(1)

# ==================== DB setup ====================
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
    pool_size=10,       # giới hạn pool
    max_overflow=20,    # slot tạm
    pool_recycle=1800,  # tái chế kết nối lâu ngày
)
metadata = MetaData()

x_table = Table(
    "eboard_table", metadata,
    Column('symbol', String, primary_key=True),
    Column('exchange', String),
    Column('indices', String),
    Column('ceiling', Float),
    Column('floor', Float),
    Column('refPrice', Float),
    Column('buyPrice3', Float), Column('buyVol3', Float),
    Column('buyPrice2', Float), Column('buyVol2', Float),
    Column('buyPrice1', Float), Column('buyVol1', Float),
    Column('matchPrice', Float), Column('matchVol', Float),
    Column('matchChange', Float), Column('matchRatioChange', Float),
    Column('sellPrice1', Float), Column('sellVol1', Float),
    Column('sellPrice2', Float), Column('sellVol2', Float),
    Column('sellPrice3', Float), Column('sellVol3', Float),
    Column('totalVol', Float), Column('totalVal', Float),
    Column('high', Float), Column('low', Float), Column('open', Float), Column('close', Float),
    schema="history_data"
)

r_table = Table(
    "eboard_foreign", metadata,
    Column("symbol", String, primary_key=True),
    Column("buyVol", Float),
    Column("sellVol", Float),
    Column("room", Float),
    Column("buyVal", Float),
    Column("sellVal", Float),
    schema="history_data"
)

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

# ==================== Trading time check ====================
def is_trading_time():
    now = datetime.now()
    today = now.date()
    if now.weekday() >= 5 or today in holiday:
        return False
    t9h = dtime(9, 0); t12h = dtime(12, 0)
    t13h = dtime(13, 0); t15h = dtime(15, 0)
    return (t9h <= now.time() <= t12h) or (t13h <= now.time() <= t15h)

# ==================== WebSocket broadcast ====================
async def broadcast(channel, data: dict):
    dead_clients = []
    for ws in list(clients[channel]):
        try:
            await ws.send_text(orjson.dumps(data).decode())
        except Exception:
            dead_clients.append(ws)
    for ws in dead_clients:
        clients[channel].discard(ws)

def schedule_broadcast(channel: str, data: dict):
    """Gọi từ thread an toàn: chỉ schedule nếu loop đang chạy."""
    loop = getattr(app.state, "loop", None)
    if loop is None or loop.is_closed() or not loop.is_running():
        logging.warning("🔸 Loop chưa sẵn sàng/đã đóng → drop broadcast %s", channel)
        return
    try:
        asyncio.run_coroutine_threadsafe(broadcast(channel, data), loop)
    except Exception:
        logging.exception("🔸 schedule_broadcast(%s) lỗi", channel)

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

@app.websocket("/ws/eboard_table")
async def websocket_x(ws: WebSocket):
    await websocket_endpoint(ws, "X")

@app.websocket("/ws/eboard_foreign")
async def websocket_r(ws: WebSocket):
    await websocket_endpoint(ws, "R")

@app.websocket("/ws/indices")
async def websocket_mi(ws: WebSocket):
    await websocket_endpoint(ws, "MI")

@app.on_event("shutdown")
async def shutdown_event():
    for ch in clients:
        clients[ch].clear()
    logging.info("🧹 Shutdown: cleared clients")

# ==================== DB retry helper ====================
def db_upsert_with_retry(do_upsert_func, label: str, max_tries=5, base_delay=0.5):
    delay = base_delay
    for attempt in range(1, max_tries+1):
        try:
            do_upsert_func()
            return True
        except OperationalError as e:
            logging.error(f"❌ {label} DB op failed (attempt {attempt}/{max_tries}): {e}")
            time.sleep(delay)
            delay = min(delay * 2, 5.0)
        except Exception as e:
            logging.exception(f"💥 {label} unexpected DB error")
            return False
    logging.error(f"⛔ {label} DB op giving up after {max_tries} attempts")
    return False

# ==================== DB upsert functions ====================
def save_x(result):
    try:
        c = result["content"]
        indices = c.get("indices")
        if isinstance(indices, list):
            indices = "|".join(indices)
        bp = c["buy"]["price"]; bv = c["buy"]["vol"]
        sp = c["sell"]["price"]; sv = c["sell"]["vol"]
        m = c["match"]

        row = {
            "symbol":  c["symbol"],
            "exchange": c.get("exchange"),
            "indices":  indices,
            "ceiling":  c["ceiling"], "floor": c["floor"],
            "refPrice": c["refPrice"],
            "buyPrice1": bp[0], "buyVol1": bv[0],
            "buyPrice2": bp[1], "buyVol2": bv[1],
            "buyPrice3": bp[2], "buyVol3": bv[2],
            "matchPrice": m["price"], "matchVol": m["vol"],
            "matchChange": m["change"], "matchRatioChange": m["ratioChange"],
            "sellPrice1": sp[0], "sellVol1": sv[0],
            "sellPrice2": sp[1], "sellVol2": sv[1],
            "sellPrice3": sp[2], "sellVol3": sv[2],
            "totalVol": c["totalVol"], "totalVal": c.get("totalVal"),
            "high": c["high"], "low": c["low"],
            "open": c.get("open"), "close": c.get("close"),
        }

        def _do():
            with engine.begin() as conn:
                stmt = pg_insert(x_table).values([row])
                update_dict = {k: getattr(stmt.excluded, k) for k in row if k != "symbol"}
                stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
                conn.execute(stmt)

        if db_upsert_with_retry(_do, label=f"X:{c.get('symbol','')}"):
            logging.info("✅ X upserted %s", c.get("symbol",""))

    except Exception as e:
        logging.error("❌ X DB save error: %s", e)

def save_r(result):
    try:
        c = result["content"]
        row = {
            "symbol": c["symbol"], "buyVol": c["buyVol"], "sellVol": c["sellVol"],
            "room": c["room"], "buyVal": c["buyVal"], "sellVal": c["sellVal"],
        }

        def _do():
            with engine.begin() as conn:
                stmt = pg_insert(r_table).values([row])
                update_dict = {k: getattr(stmt.excluded, k) for k in row if k != "symbol"}
                stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
                conn.execute(stmt)

        if db_upsert_with_retry(_do, label=f"R:{c.get('symbol','')}"):
            logging.info("✅ R upserted %s", c.get("symbol",""))

    except Exception as e:
        logging.error("❌ R DB save error: %s", e)

def save_mi(result):
    try:
        c = result["content"]
        raw = c.get("advancersDecliners")
        if isinstance(raw, dict):
            seq = [raw.get("Advances"), raw.get("NoChanges"), raw.get("Declines")]
        else:
            seq = list(raw) if raw is not None else []
        adv, nc, dec = (seq + [None, None, None])[:3]
        row = {
            "symbol": c["symbol"], "point": c["point"], "change": c["change"],
            "ratioChange": c["ratioChange"], "totalVol": c["totalVol"], "totalVal": c["totalVal"],
            "advancers": adv, "noChange": nc, "decliners": dec,
        }

        def _do():
            with engine.begin() as conn:
                stmt = pg_insert(mi_table).values([row])
                update_dict = {k: getattr(stmt.excluded, k) for k in row if k != "symbol"}
                stmt = stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict)
                conn.execute(stmt)

        if db_upsert_with_retry(_do, label=f"MI:{c.get('symbol','')}"):
            logging.info("✅ MI upserted %s", c.get("symbol",""))

    except Exception as e:
        logging.error("❌ MI DB save error: %s", e)

# ==================== Helpers ====================
def find_exchange(symbol: str, exchange_map: dict):
    for exchange, symbols in exchange_map.items():
        if symbol in symbols:
            return exchange
    return None

def find_indice(symbol: str, indices_map: dict):
    indices_list=[]
    for indices, symbols in indices_map.items():
        if symbol in symbols:
            indices_list.append(indices)
    return indices_list if indices_list else None

def _k(x):
    if x is None: return None
    return x/1000

def _safe_ratio(last_price, ref_price):
    if last_price is None or ref_price in (None,0): return None
    return (last_price-ref_price)/ref_price*100.0

# ==================== Message handlers ====================
def on_message_X(message):
    global last_msg_time
    try:
        data = orjson.loads(message.get("Content","{}"))
        symbol = data['Symbol']
        exchange = find_exchange(symbol, exchange_map)
        indices = find_indice(symbol, indices_map)
        result = {
            'function':'eboard_table',
            'content': {
                'symbol': symbol,'exchange': exchange,'indices': indices,
                'ceiling': _k(data.get('Ceiling')),'floor': _k(data.get('Floor')),
                'refPrice': _k(data.get('RefPrice')),
                'buy':{'price': [_k(data.get('BidPrice1')),_k(data.get('BidPrice2')),_k(data.get('BidPrice3'))],
                       'vol': [data.get('BidVol1'), data.get('BidVol2'), data.get('BidVol3')]},
                'match':{'price': _k(data.get('LastPrice')),'vol': data.get('LastVol'),
                         'change': _k(data.get('Change')),
                         'ratioChange': _safe_ratio(_k(data.get('LastPrice')), _k(data.get('RefPrice')))},
                'sell':{'price': [_k(data.get('AskPrice1')),_k(data.get('AskPrice2')),_k(data.get('AskPrice3'))],
                        'vol': [data.get('AskVol1'), data.get('AskVol2'), data.get('AskVol3')]},
                'totalVol': data.get('TotalVol'),'totalVal': data.get('TotalVal'),
                'high': _k(data.get('High')),'low': _k(data.get('Low')),
                'open': _k(data.get('Open')),'close': _k(data.get('Close')),
            }
        }
        schedule_broadcast("X", result)
        save_x(result); last_msg_time["X"] = time.time()
    except Exception:
        logging.exception("❗ X message error")

def on_message_R(message):
    global last_msg_time
    try:
        data = orjson.loads(message.get("Content","{}"))
        symbol = data['Symbol']
        result = {'function': 'eboard_foreign','content': {
            'symbol': symbol,'buyVol': data.get('BuyVol'),'sellVol': data.get('SellVol'),
            'room': data.get('CurrentRoom'),'buyVal': data.get('BuyVal'),'sellVal': data.get('SellVal')}}
        schedule_broadcast("R", result)
        save_r(result); last_msg_time["R"] = time.time()
    except Exception:
        logging.exception("❗ R message error")

def on_message_MI(message):
    global last_msg_time
    try:
        data = orjson.loads(message.get("Content","{}"))
        symbol = data.get('IndexId')
        if symbol == 'HNXUpcomIndex': symbol = 'UPCOMINDEX'
        elif symbol == 'HNXIndex': symbol = 'HNXINDEX'
        result = {'function': 'indices','content': {
            'symbol': symbol,'point': data.get('IndexValue'),'change': data.get('Change'),
            'ratioChange': data.get('RatioChange'),'totalVol': data.get('AllQty'),'totalVal': data.get('AllValue'),
            'advancersDecliners': [(data.get('Advances') or 0)+(data.get('Ceilings') or 0),
                                   data.get('NoChanges'),
                                   (data.get('Declines') or 0)+(data.get('Floors') or 0)]}}
        schedule_broadcast("MI", result)
        save_mi(result); last_msg_time["MI"] = time.time()
    except Exception:
        logging.exception("❗ MI message error")

# ==================== Stream supervisor (auto-reconnect + token throttling) ====================
def on_error(error, channel_name=None):
    if channel_name:
        logging.error(f"❗ [{channel_name}] WebSocket error: {error}")
    else:
        logging.error(f"❗ WebSocket error: {error}")

def stream_supervisor(channel_name, on_message_func, stream_code, open_hours_only=False):
    backoff = 1.0
    backoff_max = 30.0
    jitter_s = (0.0, 1.0)
    while True:
        try:
            if open_hours_only and not is_trading_time():
                time.sleep(60)
                continue

            # ----- hạn chế đồng thời khi xin token -----
            client = None
            for attempt in range(1, 6):  # thử tối đa 5 lần
                try:
                    acquired = TOKEN_FETCH_SEM.acquire(timeout=10)
                    if not acquired:
                        raise RuntimeError("Token semaphore timeout")
                    try:
                        # rải đều yêu cầu token một chút
                        time.sleep(random.uniform(0.0, 0.3))
                        client = MarketDataClient(config)     # gọi /api/v2/Market/AccessToken
                        break  # OK
                    finally:
                        TOKEN_FETCH_SEM.release()
                except Exception as e:
                    sleep_s = min(backoff, backoff_max) + random.uniform(*jitter_s)
                    logging.error(f"🔐 [{channel_name}] get token fail (try {attempt}/5): {e}. Retry in {sleep_s:.1f}s")
                    time.sleep(sleep_s)
                    backoff = min(backoff * 2, backoff_max)

            if client is None:
                # bỏ qua vòng này, tiếp tục vòng while để thử lại
                continue

            mm = MarketDataStream(config, client)
            done_evt = threading.Event()

            def _run_once():
                try:
                    logging.info(f"🚀 [{channel_name}] Connecting {stream_code} ...")
                    mm.start(on_message_func, lambda e: on_error(e, channel_name), stream_code)
                except Exception as e:
                    logging.exception(f"💥 [{channel_name}] mm.start() exception: {e}")
                finally:
                    done_evt.set()
                    logging.warning(f"🔌 [{channel_name}] stream ended")

            t = threading.Thread(target=_run_once, name=f"{channel_name}-ws", daemon=True)
            t.start()

            while not done_evt.is_set():
                time.sleep(0.2)

            sleep_s = min(backoff, backoff_max) + random.uniform(*jitter_s)
            logging.info(f"🔁 [{channel_name}] Reconnecting in {sleep_s:.1f}s ...")
            time.sleep(sleep_s)
            backoff = min(backoff * 2, backoff_max)

        except Exception as e:
            logging.exception(f"⚠️ [{channel_name}] supervisor loop exception: {e}")
            sleep_s = min(backoff, backoff_max) + random.uniform(*jitter_s)
            time.sleep(sleep_s)
            backoff = min(backoff * 2, backoff_max)

def start_stream(channel_name, on_message_func, stream_code, open_hours_only=False, delay_first_connect_s=0.0):
    def _target():
        if delay_first_connect_s > 0:
            time.sleep(delay_first_connect_s)
        # GLOBAL_CONN_SEM.acquire()
        # try:
            stream_supervisor(channel_name, on_message_func, stream_code, open_hours_only=open_hours_only)
        # finally:
            # GLOBAL_CONN_SEM.release()
    threading.Thread(target=_target, name=f"{channel_name}-supervisor", daemon=True).start()

# ==================== Batching helpers ====================
def batch_symbols(symbols, batch_size: int, prefix: str):
    """
    Trả về list các tuple (group_list, stream_code)
    stream_code dạng: f"{prefix}" + "-".join(group)
    Ví dụ: (["ACB","VCB","HPG"], "X:ACB-VCB-HPG")
    """
    batches = []
    for i in range(0, len(symbols or []), batch_size):
        group = symbols[i:i+batch_size]
        code = prefix + "-".join(group)
        batches.append((group, code))
    return batches

# ==================== FastAPI startup (batched per channel) ====================
@app.on_event("startup")
async def startup_event():
    loop = asyncio.get_running_loop()
    app.state.loop = loop

    # Tuỳ chỉnh kích thước batch & nhịp mở kết nối (stagger)
    X_BATCH_SIZE = 50     # số mã / 1 kết nối X (giảm tổng kết nối)
    R_BATCH_SIZE = 50     # số mã / 1 kết nối R
    MI_BATCH_SIZE = 5     # số chỉ số / 1 kết nối MI
    STAGGER = 0.4          # giãn cách mở kết nối ~5 conn/giây

    # ===== X: batch thành "X:ACB-VCB-HPG-..." =====
    if SYMBOLS_X and isinstance(SYMBOLS_X, (list, tuple)) and len(SYMBOLS_X) > 0:
        x_batches = batch_symbols(SYMBOLS_X, X_BATCH_SIZE, prefix="X:")
        for i, (_group, code) in enumerate(x_batches, 1):
            start_stream(
                channel_name=f"X-BATCH-{i:02d}",
                on_message_func=on_message_X,
                stream_code=code,
                open_hours_only=False,
                delay_first_connect_s=STAGGER * (i-1),
            )
        logging.info(f"✅ X batched connections: {len(x_batches)} (batch size={X_BATCH_SIZE})")
    else:
        logging.warning("⚠️ SYMBOLS_X trống/không hợp lệ → không mở kết nối X.")

    # ===== R: batch thành "R:ACB-VCB-HPG-..." =====
    if SYMBOLS_R and isinstance(SYMBOLS_R, (list, tuple)) and len(SYMBOLS_R) > 0:
        r_batches = batch_symbols(SYMBOLS_R, R_BATCH_SIZE, prefix="R:")
        for i, (_group, code) in enumerate(r_batches, 1):
            start_stream(
                channel_name=f"R-BATCH-{i:02d}",
                on_message_func=on_message_R,
                stream_code=code,
                open_hours_only=False,
                delay_first_connect_s=STAGGER * (i-1),
            )
        logging.info(f"✅ R batched connections: {len(r_batches)} (batch size={R_BATCH_SIZE})")
    else:
        logging.warning("⚠️ SYMBOLS_R trống/không hợp lệ → không mở kết nối R.")

    # ===== MI: batch thành "MI:VNINDEX-VN30-..." =====
    if INDICES_MI and isinstance(INDICES_MI, (list, tuple)) and len(INDICES_MI) > 0:
        mi_batches = batch_symbols(INDICES_MI, MI_BATCH_SIZE, prefix="MI:")
        for i, (_group, code) in enumerate(mi_batches, 1):
            start_stream(
                channel_name=f"MI-BATCH-{i:02d}",
                on_message_func=on_message_MI,
                stream_code=code,
                open_hours_only=False,
                delay_first_connect_s=STAGGER * (i-1),
            )
        logging.info(f"✅ MI batched connections: {len(mi_batches)} (batch size={MI_BATCH_SIZE})")
    else:
        logging.warning("⚠️ INDICES_MI trống/không hợp lệ → không mở kết nối MI.")

    logging.info("🚀 Batched per-channel streaming + WebSocket + DB services started")
