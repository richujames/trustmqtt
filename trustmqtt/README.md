# TrustMQTT

> **Zero-trust, continuous behavioral-identity verification for MQTT brokers.**

📖 **The full documentation is the [main README at the repository root](../README.md)** — architecture, how detection & enforcement work, configuration, setup & execution, data contracts, technical novelty, the audit findings, and testing.

This directory (`trustmqtt/`) is the project itself. Run all commands from here:

```bash
make up        # build & run the whole stack (Mosquitto 2.1.x + plugin, Redis, Postgres, worker, Grafana)
make up-flashmq # run the same pipeline with FlashMQ + the shared C core
make up-hivemq # run with HiveMQ CE + the Java extension (host port 1885)
make mqttx     # bounded synthetic MQTTX traffic to HiveMQ
make test      # C plugin unit tests + Python worker suite
make eval      # detection + enforcement benchmark harness
make eval-flashmq # run the benchmark against FlashMQ
```

Deep technical reference: [`docs/SPEC.md`](./docs/SPEC.md).
Broker portability design and research: [`docs/BROKER_ADAPTERS.md`](./docs/BROKER_ADAPTERS.md).

HiveMQ setup, MQTTX traffic scenarios and validation: [`docs/HIVEMQ_MQTTX.md`](./docs/HIVEMQ_MQTTX.md).

Two-week progress summary and Windows MQTTX Desktop workflow:
[`docs/TWO_WEEK_UPDATE_MQTTX_HIVEMQ.md`](./docs/TWO_WEEK_UPDATE_MQTTX_HIVEMQ.md).
