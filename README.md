# Distributed ML Task Queue

A complete Redis-backed distributed task execution platform for lightweight ML/inference workloads. It provides a FastAPI control plane, multiple independent workers, durable job state, retries, dead-lettering, worker heartbeats, operational metrics, autoscaling recommendations, a browser dashboard, load generation, tests, Docker Compose, and CI.

## Architecture

```text
Browser / Client
      |
      v
 FastAPI API  ----> Redis job state / queues
      |                    |
      |                    +--> queued
      |                    +--> processing
      |                    +--> dead-letter
      |                    +--> worker heartbeats
      |                              |
      +------------------------------+
                                     v
                               Worker replicas
                                     |
                              ML task handlers
```

Redis is the coordination and persistence layer. Workers claim jobs using a reliable processing list, update lifecycle state, retry transient failures, and move exhausted jobs to a dead-letter queue. The API never needs to run inference itself.

## Built-in task types

- `sentiment` — deterministic text sentiment example
- `vector_summary` — vector statistics and L2 norm
- `anomaly_score` — z-score anomaly detection
- `linear_predict` — linear model inference with supplied weights

The registry in `app/core.py` is intentionally small and easy to extend with real model adapters.

## Quick start with Docker

```bash
git clone https://github.com/sattipraveena3-sudo/distributed-ml-task-queue.git
cd distributed-ml-task-queue

docker compose up --build -d --scale worker=2
```

Open:

- Dashboard: `http://localhost:8000`
- OpenAPI docs: `http://localhost:8000/docs`
- Health: `http://localhost:8000/health`

Scale workers manually:

```bash
docker compose up -d --scale worker=6
```

The API exposes an autoscaling recommendation endpoint but deliberately does not mount the Docker socket or mutate your orchestrator.

## Local development

Start Redis first (local install or Docker):

```bash
docker run --rm -p 6379:6379 redis:7.4-alpine redis-server --appendonly yes
```

Then:

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

uvicorn app.main:app --reload
```

In another terminal start one or more workers:

```bash
python -m app.worker
python -m app.worker
```

## API examples

Submit a sentiment job:

```bash
curl -X POST http://localhost:8000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"sentiment","payload":{"text":"excellent reliable service"},"max_retries":2}'
```

Submit anomaly detection:

```bash
curl -X POST http://localhost:8000/api/jobs \
  -H 'Content-Type: application/json' \
  -d '{"task_type":"anomaly_score","payload":{"values":[10,11,9,10,10.5],"value":24,"threshold":2.5}}'
```

Important endpoints:

```text
POST /api/jobs
GET  /api/jobs
GET  /api/jobs/{id}
POST /api/jobs/{id}/cancel
POST /api/jobs/{id}/retry
GET  /api/workers
GET  /api/metrics
POST /api/scale/recommendation
GET  /api/tasks
GET  /health
```

## Job lifecycle

```text
queued -> running -> complete
   |          |
   |          +-> queued (retry)
   |                    |
   +-> cancelled        +-> dead_letter (retries exhausted)
                                   |
                                   +-> queued (manual retry)
```

Every job stores timestamps, attempts, configured retry count, worker id, result and error information.

## Reliability behavior

Workers move an id from the queue to a processing list before execution. Successful jobs are acknowledged by removing the processing entry. Failures are either requeued or dead-lettered. Worker presence is reported through expiring Redis heartbeat keys, so crashed workers naturally disappear from the dashboard.

For a production deployment, the next hardening layer would normally include processing-list recovery for workers killed mid-task, Redis Sentinel/Cluster, authentication, rate limiting, tracing, Prometheus metrics, and Kubernetes HPA/KEDA integration.

## Load test

```bash
python scripts/load_test.py --jobs 500
```

The generator submits a mixed burst of all built-in task types. Watch the dashboard while changing worker replicas.

## Tests

```bash
pytest
```

Tests cover task validation and results, scaling policy, API submission/cancellation, full in-memory API → worker → result flow, worker heartbeats, dead-letter behavior, and manual retry.

## Developer shortcuts

```bash
make install
make test
make run
make worker
make compose-up
make load
make compose-down
```

## Repository layout

```text
app/
  core.py       task registry and ML handlers
  store.py      Redis + in-memory queue/state backends
  worker.py     reliable worker lifecycle
  main.py       FastAPI control plane
  cli.py        API CLI entry point
  static/       operations dashboard
scripts/
  load_test.py  burst traffic generator
tests/          core + end-to-end tests
.github/        CI pipeline
Dockerfile
docker-compose.yml
Makefile
.env.example
```

## CI

GitHub Actions runs the complete test suite, compiles the application, and builds the Docker image on pull requests. Pushes to `main` run the same checks.

## License

MIT.
