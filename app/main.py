from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.core import TASKS, TaskValidationError, execute_task, scaling_decision
from app.store import RedisStore

STATIC_DIR = Path(__file__).parent / "static"


class JobRequest(BaseModel):
    task_type: str
    payload: dict[str, Any]
    max_retries: int = Field(default=2, ge=0, le=10)


class ScaleRequest(BaseModel):
    current_workers: int = Field(default=1, ge=0, le=100)
    min_workers: int = Field(default=1, ge=0, le=100)
    max_workers: int = Field(default=12, ge=1, le=100)
    jobs_per_worker: int = Field(default=5, ge=1, le=1000)


def create_app(store=None) -> FastAPI:
    task_store = store or RedisStore()
    app = FastAPI(
        title="Distributed ML Task Queue",
        version="2.0.0",
        description="Redis-backed distributed task execution with retries, dead-lettering, worker heartbeats and observability.",
    )
    app.state.store = task_store
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def home():
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/health")
    def health():
        try:
            backend = task_store.ping()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"queue backend unavailable: {exc}") from exc
        return {"status": "ok", "queue_backend": backend, "version": "2.0.0"}

    @app.get("/api/tasks")
    def task_types():
        return {"task_types": sorted(TASKS)}

    @app.post("/api/jobs", status_code=202)
    def submit_job(request: JobRequest):
        if request.task_type not in TASKS:
            raise HTTPException(status_code=400, detail=f"unknown task type: {request.task_type}")
        try:
            # Validate before enqueueing so obvious user errors do not consume worker retries.
            execute_task(request.task_type, request.payload)
        except TaskValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return task_store.submit(request.task_type, request.payload, request.max_retries)

    @app.get("/api/jobs")
    def list_jobs(limit: int = Query(default=100, ge=1, le=1000), status: str | None = None):
        jobs = task_store.list_jobs(limit=limit)
        if status:
            jobs = [job for job in jobs if job["status"] == status]
        return {"jobs": jobs, "count": len(jobs)}

    @app.get("/api/jobs/{job_id}")
    def get_job(job_id: str):
        job = task_store.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        return job

    @app.post("/api/jobs/{job_id}/retry")
    def retry_job(job_id: str):
        job = task_store.retry(job_id)
        if not job:
            raise HTTPException(status_code=409, detail="job is not retryable")
        return job

    @app.post("/api/jobs/{job_id}/cancel")
    def cancel_job(job_id: str):
        job = task_store.cancel(job_id)
        if not job:
            raise HTTPException(status_code=409, detail="only queued jobs can be cancelled")
        return job

    @app.get("/api/workers")
    def workers():
        workers = task_store.workers()
        return {"workers": workers, "count": len(workers)}

    @app.get("/api/metrics")
    def metrics():
        return task_store.stats()

    @app.post("/api/scale/recommendation")
    def scale_recommendation(request: ScaleRequest):
        if request.min_workers > request.max_workers:
            raise HTTPException(status_code=422, detail="min_workers cannot exceed max_workers")
        depth = task_store.stats()["queue_depth"]
        target = scaling_decision(depth, request.current_workers, request.min_workers, request.max_workers, request.jobs_per_worker)
        return {
            "queue_depth": depth,
            "current_workers": request.current_workers,
            "recommended_workers": target,
            "policy": {"min": request.min_workers, "max": request.max_workers, "jobs_per_worker": request.jobs_per_worker},
            "note": "Recommendation only; use your orchestrator (Compose/Kubernetes) to apply replica changes.",
        }

    return app


app = create_app()
