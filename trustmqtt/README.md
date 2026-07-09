# TrustMQTT

**TrustMQTT** is a zero-trust, continuous behavioral-identity verification system for MQTT brokers. MQTT 5's auth answers "is this the right device?"; TrustMQTT answers "is this device still behaving like itself?" — and applies graduated, automatic countermeasures the moment it isn't.

The full technical specification — architecture, data contracts, thresholds, and build phases — lives in **[docs/SPEC.md](./docs/SPEC.md)**. This README is the short version.

## How it works

A custom C plugin hooks directly into Eclipse Mosquitto 2.1.x and ships resolved-semantic events (topic, QoS, retain, session state, MQTT5 properties — never raw payloads or wire-level frame details) to Redis Streams in microseconds, without blocking the broker. A Python worker consumes that stream and, per client, per 60s window:

1. **Behavioral Contract Engine** — learns a first-order Markov model over each client's event sequence and scores how well new behavior fits the learned contract.
2. **Statistical Drift Scorer** — an IsolationForest + OneClassSVM ensemble trained per cohort, catching feature-space anomalies (rate, payload size, topic entropy, keepalive conformance, ...).
3. **Fleet-level drift** — a coordinated-compromise check across the whole fleet, for botnet-style attacks that per-client scoring alone would miss.
4. **Policy engine** — fuses the three signals into a single trust score, with hysteresis, and issues a graduated verdict: ALLOW → WATCH → THROTTLE → QUARANTINE → KICK.
5. **Incident service** — on escalation, persists the incident, redacts anything identifying (client IDs, IPs, topic secrets — all stripped/pseudonymized before it ever leaves the process), and asks NVIDIA NIM for a plain-language report for the operator (falling back to a deterministic template if the LLM is disabled or unreachable).

Verdicts flow back to the plugin's in-memory cache (refreshed on a timer, never blocking a callback), which enforces them synchronously on the broker's ACL path — token-bucket throttling, topic quarantine, or a hard kick.

```
Devices --MQTT--> Mosquitto + C plugin --Redis Streams--> Python worker (FSM/drift/fleet/policy)
                        ^                                          |
                        └───────────── verdict cache ◄─── Redis ◄──┘
                                                                    |
                                                     Postgres + Grafana, NVIDIA NIM (redacted)
```

See [docs/SPEC.md §2](./docs/SPEC.md#2-architecture) for the full diagram and sequence flow.

## Repository layout

- `plugin/` — the C plugin that hooks into Mosquitto (event capture, verdict cache, enforcement).
- `tmq_worker/` — the Python service: ingest, feature engineering, FSM, drift scoring, fleet baseline, policy, verdicts, incidents/redaction/LLM reporting, training CLI, benchmark harness.
- `docker/mosquitto/` — Dockerfile (builds Mosquitto 2.1.x from source + the plugin) and broker config.
- `config/tmq.yaml` — all thresholds, weights, and windows (no magic numbers in code).
- `grafana/provisioning/` — fleet overview, client drill-down, and system-health dashboards.
- `migrations/` — Alembic migrations for the Postgres schema.
- `eval/` — synthetic attack scenarios and benchmark results for the evaluation harness.
- `traffic-simulator/` — generates normal + adversarial MQTT traffic for local testing and building training baselines.

## Setup & execution

### 1. Requirements
- Docker and Docker Compose
- A free NVIDIA NIM API key from [build.nvidia.com](https://build.nvidia.com) (optional — incidents fall back to a deterministic report if unset)

### 2. Configuration
Copy `.env.example` to `.env` and fill in `NVIDIA_API_KEY` if you want real LLM-written incident reports; everything else has a working default for local use.

### 3. Build & run
```bash
make up      # equivalent to: docker compose up -d --build
```
This brings up Mosquitto (2.1.x + the TrustMQTT plugin), Redis, Postgres, the scoring worker, and Grafana.

### 4. Generate traffic and train a baseline
```bash
python traffic-simulator/run_simulation.py --duration 120   # normal traffic, builds the FSM/feature baseline
make train                                                  # fits per-cohort drift models from recorded feature windows
```

### 5. Run the benchmark/eval harness
```bash
make eval    # python -m tmq_worker.replay --report — see docs/SPEC.md §9
```

### 6. Tests
```bash
make test    # C plugin unit tests (ring buffer, verdict cache, token bucket, enforcement) + Python test suite
```
