# common_shared.py
import logging
from datetime import date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ============ Logging ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ============ DB ============
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:%40Vns123456@videv.cloud:5432/vnsfintech",
    echo=False,
    pool_pre_ping=True,
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
    Column("refPoint", Float),
    Column("change", Float),
    Column("ratioChange", Float),
    Column("totalMatchVol", Float),
    Column("totalMatchVal", Float),
    Column("totalDealVol", Float),
    Column("totalDealVal", Float),
    Column("totalVol", Float),
    Column("totalVal", Float),
    Column("advancers", Integer),
    Column("noChange", Integer),
    Column("decliners", Integer),
    Column("open", Float),
    Column("close", Float),
    Column("high", Float),
    Column("low", Float),
    Column("vol", Float),
    Column("val", Float),
    schema="history_data"
)

metadata.create_all(engine)

# ============ Upsert helpers ============
def upsert_x(row: dict):
    with engine.begin() as conn:
        stmt = pg_insert(x_table).values([row])
        update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict))

def upsert_r(row: dict):
    with engine.begin() as conn:
        stmt = pg_insert(r_table).values([row])
        update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict))

def upsert_mi(row: dict):
    with engine.begin() as conn:
        stmt = pg_insert(mi_table).values([row])
        update_dict = {k: getattr(stmt.excluded, k) for k in row.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(index_elements=["symbol"], set_=update_dict))

# ============ Trading day helper (nếu cần) ============
HOLIDAYS = [date(2026, 1, 1)]
