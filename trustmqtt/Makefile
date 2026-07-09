.PHONY: build up down logs test test-plugin test-worker eval train fingerprint

# Override on the command line, e.g. `make eval SCENARIO=eval/scenarios/coordinated_drift_30_clients.yaml`
SCENARIO ?= eval/scenarios/credential_reuse_hijack.yaml

build:
	docker compose build

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f

# Runs both the C plugin unit tests and the Python worker test suite.
test: test-plugin test-worker

test-plugin:
	$(MAKE) -C plugin/tests test

test-worker:
	pip install -q -r tmq_worker/requirements.txt -r tmq_worker/requirements-dev.txt
	PYTHONPATH=. pytest tmq_worker/tests -q

train:
	docker compose run --rm tmq-worker python -m tmq_worker.train

fingerprint:
	docker compose run --rm tmq-worker python -m tmq_worker --fingerprint-only

eval:
	docker compose up -d mosquitto redis postgres
	docker compose run --rm tmq-worker python -m tmq_worker.replay \
		--scenario $(SCENARIO) --broker-host mosquitto --report
