# Distributed ML Task Queue

I built this Redis-backed worker system to make queue pressure and scaling decisions visible. API containers enqueue sentiment-classification jobs, independent workers process them with variable latency, SQLite records operational samples, and the dashboard plots queue depth and worker targets. The load generator sends bursty traffic.

```bash
docker compose up --build --scale worker=2
python scripts/load_test.py --jobs 100
```

Open `http://localhost:8000`. The autoscaler is an explicit simulation: it computes target replica counts from queue depth but does not control the Docker socket. Operators apply replicas with `docker compose up -d --scale worker=N`. Keeping orchestration advisory avoids mounting a privileged Docker socket into the application.

Tests cover classification, queue-pressure decisions, and metrics persistence. Run `pytest` after installing requirements.

Limitations are single Redis, SQLite metrics, polling, no job retries/dead-letter queue, and simulated rather than autonomous container scaling. Next steps are Celery acknowledgements, Prometheus, WebSockets, Kubernetes HPA, idempotency, tracing, retry policies, and authenticated job submission.

Suggested commits: `set up services`, `add Redis queue`, `add inference worker`, `persist metrics`, `implement scaling policy`, `add job API`, `build live dashboard`, `add burst load generator`, `add tests`, `add Compose replicas`, `document scaling boundaries`.

```bash
git init -b main
git add app/core.py app/worker.py && git commit -m "add queue worker and scaling policy"
git add app/main.py app/static && git commit -m "add job API and dashboard"
git add scripts tests && git commit -m "add load generator and tests"
git add Dockerfile docker-compose.yml && git commit -m "add Redis Compose stack"
git add README.md && git commit -m "document scaling boundaries"
gh repo create distributed-ml-task-queue --public --source=. --remote=origin
git push -u origin main
```

MIT licensed.
