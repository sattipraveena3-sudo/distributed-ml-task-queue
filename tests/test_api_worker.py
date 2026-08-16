from fastapi.testclient import TestClient

from app.main import create_app
from app.store import MemoryStore
from app.worker import Worker


def test_job_lifecycle_end_to_end():
    store = MemoryStore()
    client = TestClient(create_app(store))
    response = client.post('/api/jobs', json={
        'task_type': 'sentiment',
        'payload': {'text': 'great helpful service'},
        'max_retries': 1,
    })
    assert response.status_code == 202
    job_id = response.json()['id']
    assert client.get(f'/api/jobs/{job_id}').json()['status'] == 'queued'

    worker = Worker(store=store, worker_id='test-worker')
    worker.process_one(timeout=0)
    completed = client.get(f'/api/jobs/{job_id}').json()
    assert completed['status'] == 'complete'
    assert completed['result']['label'] == 'positive'
    assert client.get('/api/metrics').json()['completed'] == 1
    assert client.get('/api/workers').json()['count'] == 1


def test_validation_and_cancel():
    store = MemoryStore()
    client = TestClient(create_app(store))
    bad = client.post('/api/jobs', json={'task_type': 'vector_summary', 'payload': {'values': []}})
    assert bad.status_code == 422
    queued = client.post('/api/jobs', json={'task_type': 'linear_predict', 'payload': {'features': [1], 'weights': [2]}}).json()
    cancelled = client.post(f"/api/jobs/{queued['id']}/cancel")
    assert cancelled.status_code == 200
    assert cancelled.json()['status'] == 'cancelled'


def test_dead_letter_and_retry():
    store = MemoryStore()
    job = store.submit('sentiment', {'text': ''}, max_retries=0)
    worker = Worker(store=store, worker_id='test-worker')
    worker.process_one(timeout=0)
    failed = store.get(job['id'])
    assert failed['status'] == 'dead_letter'
    retried = store.retry(job['id'])
    assert retried['status'] == 'queued'
