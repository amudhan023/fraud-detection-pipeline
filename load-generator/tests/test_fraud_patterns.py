"""Unit tests for fraud pattern generators."""

import sys
import os
import math

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import fraud_patterns


def test_velocity_burst_returns_correct_count():
    txns = fraud_patterns.velocity_burst(count=15)
    assert len(txns) == 15
    # All same account
    assert len({t["account_id"] for t in txns}) == 1


def test_velocity_burst_uses_fraud_merchant():
    txns = fraud_patterns.velocity_burst(count=5)
    assert all(t["merchant_id"] == fraud_patterns.FRAUD_MERCHANT for t in txns)


def test_amount_spike_structure():
    txns = fraud_patterns.amount_spike()
    # 8 history + 1 spike = 9
    assert len(txns) == 9
    spike = txns[-1]
    history_amounts = [t["amount"] for t in txns[:-1]]
    avg = sum(history_amounts) / len(history_amounts)
    # spike must be >> normal amounts
    assert spike["amount"] > avg * 5


def test_impossible_travel_two_txns():
    txns = fraud_patterns.impossible_travel()
    assert len(txns) == 2
    lat1, lon1 = txns[0]["latitude"], txns[0]["longitude"]
    lat2, lon2 = txns[1]["latitude"], txns[1]["longitude"]
    # San Francisco vs Moscow — should be ~9000 km apart
    dist = _haversine(lat1, lon1, lat2, lon2)
    assert dist > 5000, f"Expected > 5000 km apart, got {dist:.1f}"


def test_all_patterns_respects_batch_size():
    txns = fraud_patterns.all_patterns(fraud_pct=10, batch_size=100)
    # At least 100 rows (fraud patterns may add extra)
    assert len(txns) >= 100


def test_all_patterns_no_fraud_merchant_in_normal_batch():
    """At 0% fraud-pct, normal transactions must not use the fraud merchant."""
    txns = fraud_patterns.all_patterns(fraud_pct=0, batch_size=50)
    fraud = [t for t in txns if t["merchant_id"] == fraud_patterns.FRAUD_MERCHANT]
    assert len(fraud) == 0, (
        f"Expected 0 fraud-merchant txns at 0% fraud-pct, got {len(fraud)}"
    )


def test_all_patterns_fraud_merchant_appears_at_high_pct():
    """At 100% fraud-pct, at least one fraud-merchant transaction must appear."""
    txns = fraud_patterns.all_patterns(fraud_pct=100, batch_size=20)
    fraud = [t for t in txns if t["merchant_id"] == fraud_patterns.FRAUD_MERCHANT]
    assert len(fraud) > 0


def test_transaction_has_required_fields():
    txns = fraud_patterns.all_patterns(fraud_pct=5, batch_size=10)
    required = {"transaction_id", "account_id", "merchant_id", "amount",
                "currency", "latitude", "longitude", "status", "event_time"}
    for t in txns:
        assert required.issubset(t.keys()), f"Missing fields in {t}"


def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
