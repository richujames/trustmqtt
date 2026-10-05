.PHONY: build up up-flashmq down logs test test-plugin test-worker eval eval-flashmq train fingerprint

# Override on the command line, e.g. `make eval SCENARIO=eval/scenarios/coordinated_drift_30_clients.yaml`
SCENARIO ?= eval/scenarios/credential_reuse_hijack.yaml
MQTTX_ARGS ?= --scenario normal --clients 10 --messages 1000

.PHONY: up-hivemq eval-hivemq mqttx test-hivemq test-mqttx

up-hivemq:
	docker compose --profile hivemq up -d --build hivemq redis postgres tmq-worker grafana

eval-hivemq:
	docker compose --profile hivemq up -d hivemq redis postgres tmq-worker
	docker compose run --rm tmq-worker python -m tmq_worker.replay --scenario $(SCENARIO) --broker-host hivemq --report

mqttx:
	docker compose --profile traffic run --rm --build mqttx $(MQTTX_ARGS)

test-hivemq:
	mvn -B -f hivemq-extension/pom.xml verify

test-mqttx:
	cd traffic-simulator/mqttx && npm ci && npm test

build:
	docker compose build

up:
	docker compose up -d --build

up-flashmq:
	docker compose --profile flashmq up -d --build flashmq redis postgres tmq-worker grafana

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

eval-flashmq:
	docker compose --profile flashmq up -d flashmq redis postgres tmq-worker
	docker compose --profile flashmq run --rm tmq-worker python -m tmq_worker.replay \
		--scenario $(SCENARIO) --broker-host flashmq --report
