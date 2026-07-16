# TrustMQTT

> **Zero-trust, continuous behavioral-identity verification for MQTT brokers.**

📖 **The full documentation is the [main README at the repository root](../README.md)** — architecture, how detection & enforcement work, configuration, setup & execution, data contracts, technical novelty, the audit findings, and testing.

This directory (`trustmqtt/`) is the project itself. Run all commands from here:

```bash
make up        # build & run the whole stack (Mosquitto 2.1.x + plugin, Redis, Postgres, worker, Grafana)
make test      # C plugin unit tests + Python worker suite
make eval      # detection + enforcement benchmark harness
```

Deep technical reference: [`docs/SPEC.md`](./docs/SPEC.md).
