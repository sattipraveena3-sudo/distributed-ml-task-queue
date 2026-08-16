PYTHON ?= python

.PHONY: install test run worker compose-up compose-down load

install:
	$(PYTHON) -m pip install -r requirements.txt

test:
	pytest

run:
	uvicorn app.main:app --reload

worker:
	$(PYTHON) -m app.worker

compose-up:
	docker compose up --build -d --scale worker=2

compose-down:
	docker compose down

load:
	$(PYTHON) scripts/load_test.py --jobs 100
