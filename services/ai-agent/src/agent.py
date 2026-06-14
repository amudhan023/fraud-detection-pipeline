import anthropic
import json
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-8"

_ANALYSIS_TOOL = {
    "name": "record_fraud_analysis",
    "description": (
        "Record the structured fraud analysis result after evaluating all "
        "transaction details, anomaly evidence, account profile, and merchant risk."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "is_fraud": {
                "type": "boolean",
                "description": "Whether this transaction is considered fraudulent.",
            },
            "confidence": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "Confidence level in the assessment (0.0 = uncertain, 1.0 = certain).",
            },
            "ai_risk_score": {
                "type": "integer",
                "minimum": 0,
                "maximum": 100,
                "description": "AI-generated risk score (0 = no risk, 100 = definite fraud).",
            },
            "classification": {
                "type": "string",
                "enum": [
                    "CONFIRMED_FRAUD",
                    "LIKELY_FRAUD",
                    "SUSPICIOUS",
                    "LEGITIMATE",
                    "UNKNOWN",
                ],
                "description": "Fraud classification category.",
            },
            "reasoning": {
                "type": "string",
                "description": (
                    "Clear, concise explanation of the fraud assessment covering "
                    "the key signals that drove the decision."
                ),
            },
            "recommended_actions": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Ordered list of recommended actions from most to least urgent. "
                    "Use values like: BLOCK_ACCOUNT, DENY_TRANSACTION, MANUAL_REVIEW, "
                    "CONTACT_CUSTOMER, APPROVE_TRANSACTION, FLAG_FOR_MONITORING."
                ),
            },
        },
        "required": [
            "is_fraud",
            "confidence",
            "ai_risk_score",
            "classification",
            "reasoning",
            "recommended_actions",
        ],
    },
}


def _build_prompt(event: Dict[str, Any]) -> str:
    evidence = json.dumps(event.get("evidence") or {}, indent=2)
    breakdown = json.dumps(event.get("score_breakdown") or {}, indent=2)
    amount = event.get("amount") or 0
    credit_limit = event.get("credit_limit") or 0
    pct_of_limit = (amount / credit_limit * 100) if credit_limit else 0

    return f"""You are an expert fraud detection analyst. Evaluate the following transaction anomaly and provide a comprehensive fraud assessment.

ANOMALY ALERT:
  Type        : {event.get("reason_code", "UNKNOWN")}
  Detected at : {event.get("detected_at")}
  Evidence    :
{evidence}

TRANSACTION:
  ID          : {event.get("transaction_id")}
  Amount      : ${amount:.2f} {event.get("currency", "USD")} ({pct_of_limit:.1f}% of credit limit)
  Timestamp   : {event.get("event_time")}
  Location    : {event.get("latitude", "N/A")}, {event.get("longitude", "N/A")}

ACCOUNT:
  Owner       : {event.get("owner_name", "Unknown")}
  Tier        : {event.get("tier", "STANDARD")}
  Country     : {event.get("account_country", "Unknown")}
  Credit Limit: ${credit_limit:.2f}
  Risk Score  : {event.get("account_risk_score", 0):.2f}

MERCHANT:
  Name        : {event.get("merchant_name", "Unknown")}
  Category    : {event.get("merchant_category", "Unknown")}
  Risk Level  : {event.get("merchant_risk_category", "UNKNOWN")}
  Country     : {event.get("merchant_country", "Unknown")}

RULE-BASED SCORING:
  Risk Score  : {(event.get("rule_risk_score") or 0):.4f}
  Breakdown   :
{breakdown}

Analyze this transaction holistically. Consider:
1. The anomaly type and the supporting evidence (velocity bursts, z-score spikes, impossible geo-travel, etc.)
2. Whether the amount is unusual relative to the account's credit limit and tier
3. Merchant risk profile and cross-border patterns
4. Time-of-day risk (transactions in UTC 00:00–06:00 carry higher risk)
5. The account's historical risk score and tier

Use the record_fraud_analysis tool to submit your structured assessment.
"""


async def analyze_fraud_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Call Claude to analyze one fraud event and return the tool-use result dict."""
    client = anthropic.AsyncAnthropic()

    stream = client.messages.stream(
        model=MODEL,
        max_tokens=4096,
        thinking={"type": "adaptive"},
        tools=[_ANALYSIS_TOOL],
        tool_choice={"type": "any"},
        messages=[{"role": "user", "content": _build_prompt(event)}],
    )

    async with stream as s:
        message = await s.get_final_message()

    for block in message.content:
        if block.type == "tool_use" and block.name == "record_fraud_analysis":
            return block.input

    raise ValueError(
        f"Claude did not invoke record_fraud_analysis for event {event.get('id')}"
    )
