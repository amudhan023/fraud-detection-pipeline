"""
Fraudulent transaction pattern generators.

Each function returns a list of transaction dicts that should trigger
one or more anomaly rules in the Flink pipeline.
"""

import random
import uuid
from datetime import datetime, timezone

from db import KNOWN_ACCOUNT_IDS

# High-risk merchant (CryptoExchange X) — only used in fraud patterns
FRAUD_MERCHANT = "a1b2c3d4-0001-0001-0001-000000000004"
# Normal merchant (Amazon) — used as the "origin" in geo-travel patterns
NORMAL_MERCHANT = "a1b2c3d4-0001-0001-0001-000000000001"

# Merchants used for normal (non-fraudulent) transaction generation;
# intentionally excludes FRAUD_MERCHANT and the "dark market" entry.
NORMAL_MERCHANT_IDS = [
    "a1b2c3d4-0001-0001-0001-000000000001",  # Amazon
    "a1b2c3d4-0001-0001-0001-000000000002",  # Delta Airlines
    "a1b2c3d4-0001-0001-0001-000000000003",  # Starbucks
    "a1b2c3d4-0001-0001-0001-000000000005",  # Shell Gas
    "a1b2c3d4-0001-0001-0001-000000000006",  # Best Buy
    "a1b2c3d4-0001-0001-0001-000000000007",  # AirBnb
]


def _txn(account_id: str, merchant_id: str, amount: float,
         lat: float, lon: float, ts: datetime | None = None) -> dict:
    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "merchant_id": merchant_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "status": "PENDING",
        "event_time": (ts or datetime.now(timezone.utc)).isoformat(),
    }


def velocity_burst(account_id: str | None = None, count: int = 15) -> list[dict]:
    """
    Generate `count` rapid transactions for the same account.
    Triggers the VELOCITY anomaly rule (default: 10 txns / 60s).
    """
    acct = account_id or random.choice(KNOWN_ACCOUNT_IDS)
    lat, lon = 37.7749, -122.4194  # San Francisco
    now = datetime.now(timezone.utc)
    return [
        _txn(acct, FRAUD_MERCHANT, random.uniform(10, 200), lat, lon, now)
        for _ in range(count)
    ]


def amount_spike(account_id: str | None = None) -> list[dict]:
    """
    Establish a low-amount history then inject a huge spike.
    Triggers AMOUNT_SPIKE (z-score > 3.0).
    """
    acct = account_id or random.choice(KNOWN_ACCOUNT_IDS)
    lat, lon = 37.7749, -122.4194
    now = datetime.now(timezone.utc)
    # 8 normal transactions to build history
    history = [_txn(acct, NORMAL_MERCHANT, random.uniform(5, 50), lat, lon, now) for _ in range(8)]
    # 1 spike: 50x the normal mean (~$1250)
    spike = _txn(acct, FRAUD_MERCHANT, round(random.uniform(1000, 5000), 2), lat, lon, now)
    return history + [spike]


def impossible_travel(account_id: str | None = None) -> list[dict]:
    """
    Two transactions seconds apart, one in San Francisco, one in Moscow.
    Implies ~10 000 km/h — triggers GEO_TRAVEL (max: 900 km/h).
    """
    acct = account_id or random.choice(KNOWN_ACCOUNT_IDS)
    now = datetime.now(timezone.utc)
    txn_sf = _txn(acct, NORMAL_MERCHANT, 45.0, 37.7749, -122.4194, now)
    # Moscow, 1 second later
    from datetime import timedelta
    txn_ru = _txn(acct, FRAUD_MERCHANT, 89.0, 55.7558, 37.6173,
                  now + timedelta(seconds=1))
    return [txn_sf, txn_ru]


def all_patterns(fraud_pct: float, batch_size: int) -> list[dict]:
    """
    Generate `batch_size` transactions where approximately `fraud_pct`% are
    fraudulent pattern injections.
    """
    fraud_count = int(batch_size * fraud_pct / 100)  # 0% → no injection
    normal_count = batch_size - fraud_count

    # Normal transactions — only use low/medium risk merchants
    txns: list[dict] = [
        _txn(
            random.choice(KNOWN_ACCOUNT_IDS),
            random.choice(NORMAL_MERCHANT_IDS),
            round(random.uniform(5, 300), 2),
            random.uniform(-90, 90),
            random.uniform(-180, 180),
        )
        for _ in range(normal_count)
    ]

    # Fraudulent injections — distribute across the three rule types
    pattern_funcs = [velocity_burst, amount_spike, impossible_travel]
    for i in range(fraud_count):
        fn = pattern_funcs[i % len(pattern_funcs)]
        txns.extend(fn())

    random.shuffle(txns)
    return txns
