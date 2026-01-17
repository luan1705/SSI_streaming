import logging
from datetime import date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, text, Boolean
from sqlalchemy.dialects.postgresql import insert as pg_insert
import numpy as np
import pandas as pd

# ============ Logging ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ============ DB ============
engine = create_engine(
    "postgresql+psycopg2://vnsfintech:Vns_123456@videv.cloud:5433/vnsfintech",
    echo=False,
    pool_pre_ping=True,
)
metadata = MetaData()

alert_status=Table(
    "alert_status", metadata,
    Column('symbol', String, primary_key=True),
    # --- indicators ---
    Column("MA10", Float),
    Column("MA20", Float),
    Column("MA50", Float),
    Column("RSI", Float),
    Column("MFI", Float),

    Column("volume_10", Float),
    Column("volume_20", Float),
    Column("volume_50", Float),

    # --- MA crosses ---
    Column("ma10_cross_up", Boolean),
    Column("ma20_cross_up", Boolean),
    Column("ma50_cross_up", Boolean),
    Column("ma10_cross_down", Boolean),
    Column("ma20_cross_down", Boolean),
    Column("ma50_cross_down", Boolean),

    # --- MACD ---
    # Column("macd1", Float),
    # Column("signal_line1", Float),
    # Column("macd2", Float),
    # Column("signal_line2", Float),
    Column("macd_cross_up", Boolean),
    Column("macd_cross_down", Boolean),

    # --- above/below states ---
    Column("ma10_above", Boolean),
    Column("ma20_above", Boolean),
    Column("ma50_above", Boolean),
    Column("macd_above", Boolean),

    # --- Bollinger conditions ---
    Column("bb_upper_cross_up", Boolean),
    Column("bb_upper_cross_down", Boolean),
    Column("bb_lower_cross_down", Boolean),
    Column("bb_lower_cross_up", Boolean),
    Column("bb_upper_above", Boolean),
    Column("bb_lower_below", Boolean),

    schema="status"
)
metadata.create_all(engine)

def _py(v):
    # None
    if v is None:
        return None

    # pandas NaN/NaT
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass

    # numpy scalar -> python scalar
    if isinstance(v, (np.integer, np.floating, np.bool_)):
        return v.item()

    return v

def upsert_alert_status(row: dict):
    allowed = set(alert_status.c.keys())
    clean = {k: _py(row.get(k)) for k in allowed if k in row}

    with engine.begin() as conn:
        stmt = pg_insert(alert_status).values(clean)
        update_dict = {k: getattr(stmt.excluded, k) for k in clean.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(
            index_elements=["symbol"],
            set_=update_dict
        ))
