import logging
from datetime import date
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, text, Boolean, DateTime
from sqlalchemy.dialects.postgresql import insert as pg_insert, DOUBLE_PRECISION
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
    Column("open", DOUBLE_PRECISION),
    Column("high", DOUBLE_PRECISION),
    Column("low", DOUBLE_PRECISION),
    Column("close", DOUBLE_PRECISION),
    Column("volume", DOUBLE_PRECISION),
    Column("pivot", DOUBLE_PRECISION),

    # --- status_content order ---
    Column("sma10", DOUBLE_PRECISION),
    Column("close_cross_up_sma10", Boolean),
    Column("close_cross_down_sma10", Boolean),
    Column("pivot_cross_up_sma10", Boolean),
    Column("pivot_cross_down_sma10", Boolean),

    Column("sma20", DOUBLE_PRECISION),
    Column("close_cross_up_sma20", Boolean),
    Column("close_cross_down_sma20", Boolean),
    Column("pivot_cross_up_sma20", Boolean),
    Column("pivot_cross_down_sma20", Boolean),

    Column("sma50", DOUBLE_PRECISION),
    Column("close_cross_up_sma50", Boolean),
    Column("close_cross_down_sma50", Boolean),
    Column("pivot_cross_up_sma50", Boolean),
    Column("pivot_cross_down_sma50", Boolean),

    Column("sma10_cross_up_sma20", Boolean),
    Column("sma10_cross_down_sma20", Boolean),
    Column("sma10_cross_up_sma50", Boolean),
    Column("sma10_cross_down_sma50", Boolean),
    Column("sma20_cross_up_sma50", Boolean),
    Column("sma20_cross_down_sma50", Boolean),

    Column("ema10", DOUBLE_PRECISION),
    Column("close_cross_up_ema10", Boolean),
    Column("close_cross_down_ema10", Boolean),
    Column("pivot_cross_up_ema10", Boolean),
    Column("pivot_cross_down_ema10", Boolean),

    Column("ema20", DOUBLE_PRECISION),
    Column("close_cross_up_ema20", Boolean),
    Column("close_cross_down_ema20", Boolean),
    Column("pivot_cross_up_ema20", Boolean),
    Column("pivot_cross_down_ema20", Boolean),

    Column("ema50", DOUBLE_PRECISION),
    Column("close_cross_up_ema50", Boolean),
    Column("close_cross_down_ema50", Boolean),
    Column("pivot_cross_up_ema50", Boolean),
    Column("pivot_cross_down_ema50", Boolean),
    Column("ema100", DOUBLE_PRECISION),
    Column("ema200", DOUBLE_PRECISION),

    Column("ema10_cross_up_ema20", Boolean),
    Column("ema10_cross_down_ema20", Boolean),
    Column("ema10_cross_up_ema50", Boolean),
    Column("ema10_cross_down_ema50", Boolean),
    Column("ema20_cross_up_ema50", Boolean),
    Column("ema20_cross_down_ema50", Boolean),

    Column("RSI10", DOUBLE_PRECISION),
    Column("RSI11", DOUBLE_PRECISION),
    Column("RSI12", DOUBLE_PRECISION),
    Column("RSI13", DOUBLE_PRECISION),
    Column("RSI14", DOUBLE_PRECISION),
    Column("RSI15", DOUBLE_PRECISION),
    Column("RSI16", DOUBLE_PRECISION),
    Column("RSI17", DOUBLE_PRECISION),
    Column("RSI18", DOUBLE_PRECISION),
    Column("RSI19", DOUBLE_PRECISION),
    Column("RSI20", DOUBLE_PRECISION),
    Column("MFI10", DOUBLE_PRECISION),
    Column("MFI11", DOUBLE_PRECISION),
    Column("MFI12", DOUBLE_PRECISION),
    Column("MFI13", DOUBLE_PRECISION),
    Column("MFI14", DOUBLE_PRECISION),
    Column("MFI15", DOUBLE_PRECISION),
    Column("MFI16", DOUBLE_PRECISION),
    Column("MFI17", DOUBLE_PRECISION),
    Column("MFI18", DOUBLE_PRECISION),
    Column("MFI19", DOUBLE_PRECISION),
    Column("MFI20", DOUBLE_PRECISION),

    Column("volume_10", DOUBLE_PRECISION),
    Column("volume_20", DOUBLE_PRECISION),
    Column("volume_50", DOUBLE_PRECISION),

    Column("macd", DOUBLE_PRECISION),
    Column("signal_line", DOUBLE_PRECISION),
    Column("macd_cross_up_signal", Boolean),
    Column("macd_cross_down_signal", Boolean),
    Column("macd_cross_up_zero", Boolean),
    Column("macd_cross_down_zero", Boolean),

    Column("BB_upper", DOUBLE_PRECISION),
    Column("BB_lower", DOUBLE_PRECISION),
    Column("close_cross_up_bb_upper", Boolean),
    Column("close_cross_down_bb_upper", Boolean),
    Column("close_cross_up_bb_lower", Boolean),
    Column("close_cross_down_bb_lower", Boolean),
    Column("pivot_cross_up_bb_upper", Boolean),
    Column("pivot_cross_down_bb_upper", Boolean),
    Column("pivot_cross_up_bb_lower", Boolean),
    Column("pivot_cross_down_bb_lower", Boolean),

    Column("K1413", DOUBLE_PRECISION),
    Column("D1413", DOUBLE_PRECISION),
    Column("stoch1413_cross_up", Boolean),
    Column("stoch1413_cross_down", Boolean),

    Column("K1433", DOUBLE_PRECISION),
    Column("D1433", DOUBLE_PRECISION),
    Column("stoch1433_cross_up", Boolean),
    Column("stoch1433_cross_down", Boolean),

    Column("tk", DOUBLE_PRECISION),
    Column("ks", DOUBLE_PRECISION),
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
