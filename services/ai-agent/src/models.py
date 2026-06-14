from pydantic import BaseModel
from typing import Any, List, Optional
from datetime import datetime
from uuid import UUID
from enum import Enum


class Classification(str, Enum):
    CONFIRMED_FRAUD = "CONFIRMED_FRAUD"
    LIKELY_FRAUD = "LIKELY_FRAUD"
    SUSPICIOUS = "SUSPICIOUS"
    LEGITIMATE = "LEGITIMATE"
    UNKNOWN = "UNKNOWN"


class ActionType(str, Enum):
    APPROVE = "APPROVE"
    DENY = "DENY"
    ESCALATE = "ESCALATE"
    BLOCK_ACCOUNT = "BLOCK_ACCOUNT"


class AIFraudAnalysis(BaseModel):
    id: UUID
    anomaly_event_id: UUID
    is_fraud: bool
    confidence: float
    ai_risk_score: int
    classification: Classification
    reasoning: str
    recommended_actions: List[str]
    model_used: str
    analyzed_at: datetime


class CaseAction(BaseModel):
    id: UUID
    anomaly_event_id: UUID
    action: ActionType
    notes: Optional[str]
    performed_at: datetime


class FraudEvent(BaseModel):
    id: UUID
    transaction_id: UUID
    account_id: UUID
    reason_code: str
    evidence: Optional[Any]
    detected_at: datetime

    amount: Optional[float]
    currency: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    event_time: Optional[datetime]
    rule_risk_score: Optional[float]
    score_breakdown: Optional[Any]

    owner_name: Optional[str]
    tier: Optional[str]
    account_country: Optional[str]
    credit_limit: Optional[float]
    account_risk_score: Optional[float]

    merchant_name: Optional[str]
    merchant_category: Optional[str]
    merchant_risk_category: Optional[str]
    merchant_country: Optional[str]

    ai_analysis: Optional[AIFraudAnalysis] = None
    last_action: Optional[str] = None
    case_actions: List[CaseAction] = []


class DashboardStats(BaseModel):
    total_events: int
    high_risk_count: int
    pending_review_count: int
    confirmed_fraud_count: int
    analyzed_count: int


class RecordActionRequest(BaseModel):
    action: ActionType
    notes: Optional[str] = None


class AnalysisResult(BaseModel):
    anomaly_event_id: UUID
    status: str
    analysis: Optional[AIFraudAnalysis] = None
    error: Optional[str] = None
