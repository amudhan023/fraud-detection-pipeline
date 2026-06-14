import asyncpg
import json
import os
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://frauduser:fraudpass@postgres:5432/frauddb",
)

_pool: Optional[asyncpg.Pool] = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            DATABASE_URL, min_size=2, max_size=10, init=_init_connection
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def _to_dict(record: asyncpg.Record) -> Dict[str, Any]:
    result = {}
    for key, value in dict(record).items():
        if isinstance(value, UUID):
            result[key] = str(value)
        elif isinstance(value, Decimal):
            result[key] = float(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


_EVENTS_QUERY = """
    SELECT
        ae.id,
        ae.transaction_id,
        ae.account_id,
        ae.reason_code,
        ae.evidence,
        ae.detected_at,
        st.amount,
        t.currency,
        t.latitude,
        t.longitude,
        t.event_time,
        st.risk_score        AS rule_risk_score,
        st.score_breakdown,
        a.owner_name,
        a.tier,
        a.country            AS account_country,
        a.credit_limit,
        a.risk_score         AS account_risk_score,
        m.name               AS merchant_name,
        m.category           AS merchant_category,
        m.risk_category      AS merchant_risk_category,
        m.country            AS merchant_country,
        afa.id               AS ai_analysis_id,
        afa.is_fraud,
        afa.confidence,
        afa.ai_risk_score,
        afa.classification,
        afa.reasoning,
        afa.recommended_actions,
        afa.model_used,
        afa.analyzed_at,
        (
            SELECT action FROM fraud_case_actions fca
            WHERE fca.anomaly_event_id = ae.id
            ORDER BY fca.performed_at DESC
            LIMIT 1
        ) AS last_action
    FROM anomaly_events ae
    LEFT JOIN scored_transactions st ON ae.transaction_id = st.transaction_id
    LEFT JOIN transactions t         ON ae.transaction_id = t.transaction_id
    LEFT JOIN accounts a             ON ae.account_id = a.account_id
    LEFT JOIN merchants m            ON st.merchant_id = m.merchant_id
    LEFT JOIN ai_fraud_analysis afa  ON ae.id = afa.anomaly_event_id
"""


async def get_fraud_events(limit: int = 100, offset: int = 0) -> List[Dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        _EVENTS_QUERY + " ORDER BY ae.detected_at DESC LIMIT $1 OFFSET $2",
        limit,
        offset,
    )
    return [_to_dict(r) for r in rows]


async def get_fraud_event(event_id: str) -> Optional[Dict]:
    pool = await get_pool()
    row = await pool.fetchrow(
        _EVENTS_QUERY + " WHERE ae.id = $1",
        UUID(event_id),
    )
    if row is None:
        return None
    result = _to_dict(row)
    result["case_actions"] = await get_case_actions(event_id)
    return result


async def get_unanalyzed_events(limit: int = 10) -> List[Dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        _EVENTS_QUERY
        + """
        WHERE afa.id IS NULL
        ORDER BY ae.detected_at ASC
        LIMIT $1
        """,
        limit,
    )
    return [_to_dict(r) for r in rows]


async def get_dashboard_stats() -> Dict:
    pool = await get_pool()
    row = await pool.fetchrow("""
        SELECT
            COUNT(*)                                                        AS total_events,
            COUNT(*) FILTER (WHERE afa.ai_risk_score >= 70)                 AS high_risk_count,
            COUNT(*) FILTER (WHERE afa.id IS NULL)                          AS pending_review_count,
            COUNT(*) FILTER (WHERE afa.classification = 'CONFIRMED_FRAUD')  AS confirmed_fraud_count,
            COUNT(*) FILTER (WHERE afa.id IS NOT NULL)                      AS analyzed_count
        FROM anomaly_events ae
        LEFT JOIN ai_fraud_analysis afa ON ae.id = afa.anomaly_event_id
    """)
    return dict(row)


async def save_ai_analysis(
    anomaly_event_id: str,
    transaction_id: str,
    account_id: str,
    is_fraud: bool,
    confidence: float,
    ai_risk_score: int,
    classification: str,
    reasoning: str,
    recommended_actions: list,
    model_used: str,
) -> Dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO ai_fraud_analysis (
            anomaly_event_id, transaction_id, account_id,
            is_fraud, confidence, ai_risk_score, classification,
            reasoning, recommended_actions, model_used
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (anomaly_event_id) DO UPDATE SET
            is_fraud            = EXCLUDED.is_fraud,
            confidence          = EXCLUDED.confidence,
            ai_risk_score       = EXCLUDED.ai_risk_score,
            classification      = EXCLUDED.classification,
            reasoning           = EXCLUDED.reasoning,
            recommended_actions = EXCLUDED.recommended_actions,
            model_used          = EXCLUDED.model_used,
            analyzed_at         = NOW()
        RETURNING *
        """,
        UUID(anomaly_event_id),
        UUID(transaction_id),
        UUID(account_id),
        is_fraud,
        confidence,
        ai_risk_score,
        classification,
        reasoning,
        json.dumps(recommended_actions),
        model_used,
    )
    return _to_dict(row)


async def save_case_action(
    anomaly_event_id: str, action: str, notes: Optional[str]
) -> Dict:
    pool = await get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO fraud_case_actions (anomaly_event_id, action, notes)
        VALUES ($1, $2, $3)
        RETURNING *
        """,
        UUID(anomaly_event_id),
        action,
        notes,
    )
    return _to_dict(row)


async def get_case_actions(anomaly_event_id: str) -> List[Dict]:
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT * FROM fraud_case_actions
        WHERE anomaly_event_id = $1
        ORDER BY performed_at DESC
        """,
        UUID(anomaly_event_id),
    )
    return [_to_dict(r) for r in rows]
