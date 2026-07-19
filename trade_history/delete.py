import redis
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, inspect

# ================= CONFIG =================
REDIS_URL = "redis://root:Dnl_123456@tanhungsoft.com:6379"
POSTGRES_URL = "postgresql://root:Dnl_123456@tanhungsoft.com:5432/dnl"

SCHEMA = "trade_history"
REDIS_PREFIX = "trade_history:"
DAYS_TO_KEEP = 9

# cutoff time
cutoff = datetime.utcnow() - timedelta(days=DAYS_TO_KEEP)

print("Cutoff:", cutoff)

# =====================================================
# REDIS CLEAN
# =====================================================
print("\n====== REDIS CLEANING ======")

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
deleted_redis = 0

for key in r.scan_iter(match=f"{REDIS_PREFIX}*"):
    try:
        val = r.get(key)
        if not val:
            continue

        data = json.loads(val)

        if "time" not in data:
            continue

        ts = datetime.fromisoformat(data["time"].replace("Z",""))

        if ts < cutoff:
            r.delete(key)
            deleted_redis += 1

    except Exception:
        continue

print("Redis deleted:", deleted_redis)


# =====================================================
# POSTGRES CLEAN
# =====================================================
print("\n====== POSTGRES CLEANING ======")

engine = create_engine(POSTGRES_URL)
inspector = inspect(engine)

tables = inspector.get_table_names(schema=SCHEMA)

print("Tables found:", len(tables))

deleted_rows = 0

with engine.begin() as conn:
    for table in tables:
        try:
            result = conn.execute(text(f"""
                DELETE FROM "{SCHEMA}"."{table}"
                WHERE "time" < NOW() - INTERVAL '{DAYS_TO_KEEP} days'
            """))

            count = result.rowcount if result.rowcount else 0
            deleted_rows += count

            print(f"{table:<25} deleted {count}")

        except Exception as e:
            print(f"{table:<25} ERROR -> {e}")

print("\nTOTAL PG rows deleted:", deleted_rows)

# =====================================================
print("\nDONE CLEANING OLD DATA")