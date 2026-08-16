from __future__ import annotations

import json
import os
import time
import uuid
from collections import deque
from threading import Lock
from typing import Any

import redis

QUEUE_KEY = "mlq:queue"
PROCESSING_KEY = "mlq:processing"
DEAD_KEY = "mlq:dead"
JOB_PREFIX = "mlq:job:"
WORKER_PREFIX = "mlq:worker:"


def now() -> float:
    return time.time()


def new_job(task_type: str, payload: dict[str, Any], max_retries: int = 2) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "task_type": task_type,
        "payload": payload,
        "status": "queued",
        "attempts": 0,
        "max_retries": max(0, int(max_retries)),
        "created_at": now(),
        "updated_at": now(),
        "started_at": None,
        "finished_at": None,
        "worker_id": None,
        "result": None,
        "error": None,
    }


class RedisStore:
    def __init__(self, url: str | None = None):
        self.redis = redis.Redis.from_url(url or os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

    def ping(self) -> bool:
        return bool(self.redis.ping())

    def submit(self, task_type: str, payload: dict[str, Any], max_retries: int = 2) -> dict[str, Any]:
        job = new_job(task_type, payload, max_retries)
        encoded = json.dumps(job)
        pipe = self.redis.pipeline()
        pipe.set(JOB_PREFIX + job["id"], encoded)
        pipe.lpush(QUEUE_KEY, job["id"])
        pipe.execute()
        return job

    def get(self, job_id: str) -> dict[str, Any] | None:
        raw = self.redis.get(JOB_PREFIX + job_id)
        return json.loads(raw) if raw else None

    def save(self, job: dict[str, Any]) -> None:
        job["updated_at"] = now()
        self.redis.set(JOB_PREFIX + job["id"], json.dumps(job))

    def claim(self, timeout: int = 5) -> dict[str, Any] | None:
        job_id = self.redis.brpoplpush(QUEUE_KEY, PROCESSING_KEY, timeout=timeout)
        if not job_id:
            return None
        job = self.get(job_id)
        if job is None:
            self.redis.lrem(PROCESSING_KEY, 1, job_id)
            return None
        return job

    def ack(self, job_id: str) -> None:
        self.redis.lrem(PROCESSING_KEY, 1, job_id)

    def requeue(self, job: dict[str, Any]) -> None:
        self.ack(job["id"])
        self.save(job)
        self.redis.lpush(QUEUE_KEY, job["id"])

    def dead_letter(self, job: dict[str, Any]) -> None:
        self.ack(job["id"])
        self.save(job)
        self.redis.lpush(DEAD_KEY, job["id"])

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        keys = self.redis.scan_iter(match=JOB_PREFIX + "*")
        jobs = []
        for key in keys:
            raw = self.redis.get(key)
            if raw:
                jobs.append(json.loads(raw))
        return sorted(jobs, key=lambda item: item["created_at"], reverse=True)[:limit]

    def retry(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if not job or job["status"] not in {"failed", "dead_letter", "cancelled"}:
            return None
        job.update(status="queued", error=None, finished_at=None, worker_id=None)
        self.redis.lrem(DEAD_KEY, 0, job_id)
        self.save(job)
        self.redis.lpush(QUEUE_KEY, job_id)
        return job

    def cancel(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if not job or job["status"] != "queued":
            return None
        self.redis.lrem(QUEUE_KEY, 0, job_id)
        job.update(status="cancelled", finished_at=now())
        self.save(job)
        return job

    def heartbeat(self, worker_id: str, meta: dict[str, Any]) -> None:
        self.redis.set(WORKER_PREFIX + worker_id, json.dumps({**meta, "worker_id": worker_id, "seen_at": now()}), ex=30)

    def workers(self) -> list[dict[str, Any]]:
        result = []
        for key in self.redis.scan_iter(match=WORKER_PREFIX + "*"):
            raw = self.redis.get(key)
            if raw:
                result.append(json.loads(raw))
        return sorted(result, key=lambda item: item["worker_id"])

    def stats(self) -> dict[str, Any]:
        jobs = self.list_jobs(limit=10000)
        counts: dict[str, int] = {}
        latencies = []
        for job in jobs:
            counts[job["status"]] = counts.get(job["status"], 0) + 1
            if job.get("started_at") and job.get("finished_at"):
                latencies.append(job["finished_at"] - job["started_at"])
        latencies.sort()
        p95 = latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))] if latencies else 0.0
        return {
            "queue_depth": self.redis.llen(QUEUE_KEY),
            "processing": self.redis.llen(PROCESSING_KEY),
            "dead_letter": self.redis.llen(DEAD_KEY),
            "workers": len(self.workers()),
            "status_counts": counts,
            "completed": counts.get("complete", 0),
            "failed": counts.get("dead_letter", 0),
            "p95_latency_seconds": round(p95, 4),
        }


class MemoryStore:
    """Thread-safe in-memory backend used by tests and local demos without Redis."""

    def __init__(self):
        self.jobs: dict[str, dict[str, Any]] = {}
        self.queue: deque[str] = deque()
        self.processing: set[str] = set()
        self.dead: list[str] = []
        self.worker_data: dict[str, dict[str, Any]] = {}
        self.lock = Lock()

    def ping(self) -> bool:
        return True

    def submit(self, task_type: str, payload: dict[str, Any], max_retries: int = 2) -> dict[str, Any]:
        with self.lock:
            job = new_job(task_type, payload, max_retries)
            self.jobs[job["id"]] = job
            self.queue.appendleft(job["id"])
            return dict(job)

    def get(self, job_id: str) -> dict[str, Any] | None:
        job = self.jobs.get(job_id)
        return dict(job) if job else None

    def save(self, job: dict[str, Any]) -> None:
        job["updated_at"] = now()
        self.jobs[job["id"]] = dict(job)

    def claim(self, timeout: int = 0) -> dict[str, Any] | None:
        with self.lock:
            if not self.queue:
                return None
            job_id = self.queue.pop()
            self.processing.add(job_id)
            return self.get(job_id)

    def ack(self, job_id: str) -> None:
        self.processing.discard(job_id)

    def requeue(self, job: dict[str, Any]) -> None:
        self.ack(job["id"])
        self.save(job)
        self.queue.appendleft(job["id"])

    def dead_letter(self, job: dict[str, Any]) -> None:
        self.ack(job["id"])
        self.save(job)
        self.dead.append(job["id"])

    def list_jobs(self, limit: int = 100) -> list[dict[str, Any]]:
        return sorted((dict(job) for job in self.jobs.values()), key=lambda item: item["created_at"], reverse=True)[:limit]

    def retry(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if not job or job["status"] not in {"failed", "dead_letter", "cancelled"}:
            return None
        job.update(status="queued", error=None, finished_at=None, worker_id=None)
        self.save(job)
        self.queue.appendleft(job_id)
        return job

    def cancel(self, job_id: str) -> dict[str, Any] | None:
        job = self.get(job_id)
        if not job or job["status"] != "queued":
            return None
        try:
            self.queue.remove(job_id)
        except ValueError:
            return None
        job.update(status="cancelled", finished_at=now())
        self.save(job)
        return job

    def heartbeat(self, worker_id: str, meta: dict[str, Any]) -> None:
        self.worker_data[worker_id] = {**meta, "worker_id": worker_id, "seen_at": now()}

    def workers(self) -> list[dict[str, Any]]:
        return list(self.worker_data.values())

    def stats(self) -> dict[str, Any]:
        jobs = self.list_jobs(limit=10000)
        counts: dict[str, int] = {}
        for job in jobs:
            counts[job["status"]] = counts.get(job["status"], 0) + 1
        return {
            "queue_depth": len(self.queue), "processing": len(self.processing), "dead_letter": len(self.dead),
            "workers": len(self.worker_data), "status_counts": counts, "completed": counts.get("complete", 0),
            "failed": counts.get("dead_letter", 0), "p95_latency_seconds": 0.0,
        }
