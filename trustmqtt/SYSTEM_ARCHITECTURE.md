# TrustMQTT System Architecture

This document has been superseded by the full v2 technical specification, which now contains the architecture diagrams, sequence flow, and every data contract in one place: **[docs/SPEC.md](./docs/SPEC.md)**.

Notably outdated in the old version of this file: it described a MySQL-backed, Redis-list-queue pipeline against Mosquitto 2.0.x with no FSM, fleet-drift, policy-fusion, or incident/redaction layer. The current system (Postgres, Redis Streams, Mosquitto 2.1.x) is described in full in `docs/SPEC.md` §2 (architecture), §3–§7 (component specs), and §11 (repository layout). The prior diagrams are still visible in git history if useful for context on the project's earlier design.
