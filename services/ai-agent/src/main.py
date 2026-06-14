import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from . import agent, database
from .models import (
    AnalysisResult,
    DashboardStats,
    FraudEvent,
    RecordActionRequest,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_worker_task: Optional[asyncio.Task] = None


async def _analysis_worker() -> None:
    """Continuously pick up unanalyzed anomaly events and run Claude on them."""
    while True:
        try:
            events = await database.get_unanalyzed_events(limit=5)
            for event in events:
                await _run_analysis(event)
        except Exception as exc:
            logger.error("Analysis worker error: %s", exc)
        await asyncio.sleep(int(os.getenv("ANALYSIS_INTERVAL_SECONDS", "30")))


async def _run_analysis(event: dict) -> Optional[dict]:
    event_id = event["id"]
    try:
        logger.info("Analyzing event %s (%s)", event_id, event.get("reason_code"))
        result = await agent.analyze_fraud_event(event)
        saved = await database.save_ai_analysis(
            anomaly_event_id=event_id,
            transaction_id=event["transaction_id"],
            account_id=event["account_id"],
            is_fraud=result["is_fraud"],
            confidence=result["confidence"],
            ai_risk_score=result["ai_risk_score"],
            classification=result["classification"],
            reasoning=result["reasoning"],
            recommended_actions=result["recommended_actions"],
            model_used=agent.MODEL,
        )
        logger.info(
            "Event %s classified as %s (score=%d)",
            event_id,
            result["classification"],
            result["ai_risk_score"],
        )
        return saved
    except Exception as exc:
        logger.error("Failed to analyze event %s: %s", event_id, exc)
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _worker_task
    _worker_task = asyncio.create_task(_analysis_worker())
    logger.info("Analysis worker started")
    yield
    if _worker_task:
        _worker_task.cancel()
    await database.close_pool()
    logger.info("Shutdown complete")


app = FastAPI(title="Fraud Detection AI Agent", version="1.0.0", lifespan=lifespan)

_cors_origins = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/stats", response_model=DashboardStats)
async def get_stats():
    return await database.get_dashboard_stats()


@app.get("/api/fraud-events", response_model=List[FraudEvent])
async def list_fraud_events(limit: int = 100, offset: int = 0):
    return await database.get_fraud_events(limit=limit, offset=offset)


@app.get("/api/fraud-events/{event_id}", response_model=FraudEvent)
async def get_fraud_event(event_id: str):
    event = await database.get_fraud_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@app.post("/api/fraud-events/{event_id}/analyze", response_model=AnalysisResult)
async def analyze_event(event_id: str, background_tasks: BackgroundTasks):
    event = await database.get_fraud_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")

    result = await _run_analysis(event)
    if result is None:
        return AnalysisResult(
            anomaly_event_id=event_id,
            status="error",
            error="Analysis failed — check agent logs",
        )

    updated = await database.get_fraud_event(event_id)
    analysis_data = None
    if updated and updated.get("ai_analysis_id"):
        from .models import AIFraudAnalysis, Classification
        analysis_data = AIFraudAnalysis(
            id=updated["ai_analysis_id"],
            anomaly_event_id=event_id,
            is_fraud=updated["is_fraud"],
            confidence=updated["confidence"],
            ai_risk_score=updated["ai_risk_score"],
            classification=Classification(updated["classification"]),
            reasoning=updated["reasoning"],
            recommended_actions=updated["recommended_actions"] or [],
            model_used=updated["model_used"],
            analyzed_at=updated["analyzed_at"],
        )
    return AnalysisResult(
        anomaly_event_id=event_id,
        status="completed",
        analysis=analysis_data,
    )


@app.post("/api/fraud-events/{event_id}/action")
async def record_action(event_id: str, body: RecordActionRequest):
    event = await database.get_fraud_event(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    action = await database.save_case_action(event_id, body.action.value, body.notes)
    return {"status": "recorded", "action": action}


@app.get("/api/fraud-events/{event_id}/actions")
async def get_actions(event_id: str):
    return await database.get_case_actions(event_id)
