"""Postgres connection pool and bulk-insert helpers."""

import os
import psycopg2
from psycopg2 import pool as pg_pool


_pool: pg_pool.ThreadedConnectionPool | None = None

KNOWN_ACCOUNT_IDS = [
    "b2c3d4e5-0001-0001-0001-000000000001",
    "b2c3d4e5-0001-0001-0001-000000000002",
    "b2c3d4e5-0001-0001-0001-000000000003",
    "b2c3d4e5-0001-0001-0001-000000000004",
    "b2c3d4e5-0001-0001-0001-000000000005",
]

KNOWN_MERCHANT_IDS = [
    "a1b2c3d4-0001-0001-0001-000000000001",
    "a1b2c3d4-0001-0001-0001-000000000002",
    "a1b2c3d4-0001-0001-0001-000000000003",
    "a1b2c3d4-0001-0001-0001-000000000004",
    "a1b2c3d4-0001-0001-0001-000000000005",
    "a1b2c3d4-0001-0001-0001-000000000006",
    "a1b2c3d4-0001-0001-0001-000000000007",
    "a1b2c3d4-0001-0001-0001-000000000008",
]


def get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "frauddb"),
            user=os.getenv("POSTGRES_USER", "frauduser"),
            password=os.getenv("POSTGRES_PASSWORD", "fraudpass"),
        )
    return _pool


def insert_transactions(rows: list[dict]) -> int:
    """Bulk insert transactions. Returns number of rows inserted."""
    if not rows:
        return 0

    sql = """
        INSERT INTO transactions
          (transaction_id, account_id, merchant_id, amount, currency,
           latitude, longitude, status, event_time)
        VALUES
          (%(transaction_id)s, %(account_id)s, %(merchant_id)s, %(amount)s,
           %(currency)s, %(latitude)s, %(longitude)s, %(status)s, %(event_time)s)
        ON CONFLICT (transaction_id) DO NOTHING
    """

    pool = get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def close_pool() -> None:
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None
