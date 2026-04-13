import logging
from datetime import date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer, text, Boolean, DateTime
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
    Column("time", DateTime),
    Column("open", Float),
    Column("high", Float),
    Column("low", Float),
    Column("close", Float),
    # --- indicators ---
    Column("sma10", Float),
    Column("sma20", Float),
    Column("sma50", Float),
    Column("ema10", Float),
    Column("ema20", Float),
    Column("ema50", Float),

    Column("RSI", Float),
    Column("MFI", Float),

    Column("macd", Float),
    Column("signal_line", Float),

    Column("bb_upper", Float),
    Column("bb_lower", Float),

    Column("volume", Float),
    Column("volume_10", Float),
    Column("volume_20", Float),
    Column("volume_50", Float),

    Column("pivot", Float),

    Column("close_cross_up_sma10", Boolean),
    Column("close_cross_up_sma20", Boolean),
    Column("close_cross_up_sma50", Boolean),
    Column("close_cross_down_sma10", Boolean),
    Column("close_cross_down_sma20", Boolean),
    Column("close_cross_down_sma50", Boolean),

    Column("pivot_cross_up_sma10", Boolean),
    Column("pivot_cross_up_sma20", Boolean),
    Column("pivot_cross_up_sma50", Boolean),
    Column("pivot_cross_down_sma10", Boolean),
    Column("pivot_cross_down_sma20", Boolean),
    Column("pivot_cross_down_sma50", Boolean),

    Column("sma10_cross_up_sma20", Boolean),
    Column("sma10_cross_down_sma20", Boolean),
    Column("sma10_cross_up_sma50", Boolean),
    Column("sma10_cross_down_sma50", Boolean),
    Column("sma20_cross_up_sma50", Boolean),
    Column("sma20_cross_down_sma50", Boolean),

    Column("close_cross_up_ema10", Boolean),
    Column("close_cross_up_ema20", Boolean),
    Column("close_cross_up_ema50", Boolean),
    Column("close_cross_down_ema10", Boolean),
    Column("close_cross_down_ema20", Boolean),
    Column("close_cross_down_ema50", Boolean),

    Column("pivot_cross_up_ema10", Boolean),
    Column("pivot_cross_up_ema20", Boolean),
    Column("pivot_cross_up_ema50", Boolean),
    Column("pivot_cross_down_ema10", Boolean),
    Column("pivot_cross_down_ema20", Boolean),
    Column("pivot_cross_down_ema50", Boolean),

    Column("ema10_cross_up_ema20", Boolean),
    Column("ema10_cross_down_ema20", Boolean),
    Column("ema10_cross_up_ema50", Boolean),
    Column("ema10_cross_down_ema50", Boolean),
    Column("ema20_cross_up_ema50", Boolean),
    Column("ema20_cross_down_ema50", Boolean),

    Column("macd_cross_up_signal", Boolean),
    Column("macd_cross_down_signal", Boolean),
    Column("macd_cross_up_zero", Boolean),
    Column("macd_cross_down_zero", Boolean),

    Column("close_cross_up_bb_upper", Boolean),
    Column("close_cross_down_bb_upper", Boolean),
    Column("close_cross_up_bb_lower", Boolean),
    Column("close_cross_down_bb_lower", Boolean),

    Column("pivot_cross_up_bb_upper", Boolean),
    Column("pivot_cross_down_bb_upper", Boolean),
    Column("pivot_cross_up_bb_lower", Boolean),
    Column("pivot_cross_down_bb_lower", Boolean),

    Column("stoch_cross_up", Boolean),
    Column("stoch_cross_down", Boolean),

    Column("tk_cross_up_ks", Boolean),
    Column("tk_cross_down_ks", Boolean),

    Column("close_cross_up_cloud", Boolean),
    Column("close_cross_down_cloud", Boolean),
    Column("pivot_cross_up_cloud", Boolean),
    Column("pivot_cross_down_cloud", Boolean),

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
    row = {str(k): v for k, v in row.items()}
    allowed = set(alert_status.c.keys())
    clean = {k: _py(row.get(k)) for k in allowed if k in row}

    with engine.begin() as conn:
        stmt = pg_insert(alert_status).values(clean)
        update_dict = {k: getattr(stmt.excluded, k) for k in clean.keys() if k != "symbol"}
        conn.execute(stmt.on_conflict_do_update(
            index_elements=["symbol"],
            set_=update_dict
        ))