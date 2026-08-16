from __future__ import annotations

import os
import socket
import time
import traceback

from app.core import execute_task
from app.store import RedisStore, now


class Worker:
    def __init__(self, store=None, worker_id: str | None = None):
        self.store = store or RedisStore()
        self.worker_id = worker_id or os.getenv("WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}"
        self.poll_timeout = int(os.getenv("WORKER_POLL_TIMEOUT", "5"))
        self.simulated_latency = float(os.getenv("SIMULATED_LATENCY_SECONDS", "0"))

    def process_one(self, timeout: int | None = None) -> dict | None:
        self.store.heartbeat(self.worker_id, {"status": "idle"})
        job = self.store.claim(self.poll_timeout if timeout is None else timeout)
        if not job:
            return None

        job["attempts"] += 1
        job.update(status="running", worker_id=self.worker_id, started_at=now(), error=None)
        self.store.save(job)
        self.store.heartbeat(self.worker_id, {"status": "running", "job_id": job["id"], "task_type": job["task_type"]})
        try:
            if self.simulated_latency > 0:
                time.sleep(self.simulated_latency)
            output = execute_task(job["task_type"], job["payload"])
            job.update(status="complete", result=output, finished_at=now())
            self.store.save(job)
            self.store.ack(job["id"])
        except Exception as exc:  # workers must record task failures instead of crashing
            job["error"] = f"{type(exc).__name__}: {exc}"
            if job["attempts"] <= job["max_retries"]:
                job["status"] = "queued"
                job["worker_id"] = None
                self.store.requeue(job)
            else:
                job.update(status="dead_letter", finished_at=now())
                self.store.dead_letter(job)
            if os.getenv("WORKER_DEBUG") == "1":
                traceback.print_exc()
        finally:
            self.store.heartbeat(self.worker_id, {"status": "idle"})
        return job

    def run_forever(self) -> None:
        print(f"worker {self.worker_id} ready", flush=True)
        while True:
            self.process_one()


def main() -> None:
    Worker().run_forever()


if __name__ == "__main__":
    main()
