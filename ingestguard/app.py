from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse
from pydantic_settings import BaseSettings, SettingsConfigDict

from ingestguard.models import (
    IngestRequest,
    JobSummary,
    QuarantineItem,
    ReplayRequest,
)
from ingestguard.replayer import (
    IngestService,
    MetricsCollector,
    QuarantineAlreadyReplayedError,
    QuarantineNotFoundError,
    ReplayValidationError,
)
from ingestguard.store import IngestStore


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGESTGUARD_")

    db_url: str = "sqlite:///:memory:"
    host: str = "0.0.0.0"
    port: int = 8000


def create_app(
    db_url: str = "sqlite:///:memory:",
    store: IngestStore | None = None,
    service: IngestService | None = None,
    settings: AppSettings | None = None,
) -> FastAPI:
    """Create and configure FastAPI application instance with lifespan lifecycle."""
    active_settings = settings or AppSettings()
    effective_db_url = (
        db_url
        if db_url != "sqlite:///:memory:" or not active_settings.db_url
        else active_settings.db_url
    )
    active_store = store or IngestStore(db_url=effective_db_url)
    metrics = MetricsCollector()
    active_service = service or IngestService(store=active_store, metrics=metrics)

    active_store.init_db()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        active_store.init_db()
        yield

    app = FastAPI(
        title="IngestGuard",
        version="0.1.0",
        description="Streaming chunk ingestion, idempotent upsert, and quarantine replay service.",
        lifespan=lifespan,
    )
    app.state.store = active_store
    app.state.service = active_service
    app.state.metrics = active_service.metrics

    @app.get("/healthz", tags=["System"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "version": "0.1.0"}

    @app.get("/metrics", tags=["System"])
    async def metrics_endpoint() -> Response:
        body = active_service.metrics.render_prometheus()
        return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")

    @app.post("/v1/jobs/ingest", response_model=JobSummary, status_code=status.HTTP_201_CREATED)
    async def ingest_job(req: IngestRequest) -> JobSummary:
        return await active_service.ingest_batch(req)

    @app.get("/v1/jobs/{job_id}", response_model=JobSummary)
    async def get_job(job_id: str) -> JobSummary:
        job = await active_store.get_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found"
            )
        return JobSummary.model_validate(job)

    @app.get("/v1/jobs/{job_id}/quarantine", response_model=list[QuarantineItem])
    async def get_job_quarantine(job_id: str) -> list[QuarantineItem]:
        job = await active_store.get_job(job_id)
        if job is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail=f"Job '{job_id}' not found"
            )
        rows = await active_store.get_quarantine_rows(job_id)
        return [QuarantineItem.model_validate(r) for r in rows]

    @app.post("/v1/quarantine/{quarantine_id}/replay", response_model=None)
    async def replay_quarantine(quarantine_id: str, req: ReplayRequest) -> Response:
        try:
            record = await active_service.replay_quarantine_row(
                quarantine_id=quarantine_id,
                patched_payload=req.patched_payload,
                spec=req.schema_spec,
            )
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "id": record.id,
                    "dataset": record.dataset,
                    "record_key": record.record_key,
                    "payload_hash": record.payload_hash,
                    "payload": record.payload_json,
                    "source_job_id": record.source_job_id,
                    "updated_at": record.updated_at.isoformat(),
                },
            )
        except ReplayValidationError as exc:
            return JSONResponse(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                content={"detail": "Replay validation failed", "errors": exc.errors},
            )
        except QuarantineNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except QuarantineAlreadyReplayedError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @app.get("/v1/datasets/{dataset}/records")
    async def list_records(
        dataset: str,
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict[str, Any]]:
        rows = await active_store.get_records(dataset=dataset, limit=limit, offset=offset)
        return [
            {
                "id": r.id,
                "dataset": r.dataset,
                "record_key": r.record_key,
                "payload_hash": r.payload_hash,
                "payload": r.payload_json,
                "source_job_id": r.source_job_id,
                "updated_at": r.updated_at.isoformat(),
            }
            for r in rows
        ]

    return app
