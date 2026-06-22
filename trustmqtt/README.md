# TrustMQTT

**TrustMQTT** is a continuous behavioral identity verification system for MQTT brokers. It acts as an intrusion detection system (IDS) that dynamically learns the exact "behavioral fingerprint" of every IoT device on the network and instantly flags when a device deviates from its normal behavior.

## Overview

TrustMQTT intercepts real-time MQTT traffic directly from the broker using a custom C plugin, ensuring zero latency impact. It passes this data to a Python worker that transforms the raw packets into behavioral feature vectors. 

We use a **Hybrid AI Architecture**:
1. **Mathematical Baseline (Scikit-Learn)**: An `IsolationForest` model learns the exact mathematical rhythm of a device (e.g., payload sizes, inter-arrival timing, QoS).
2. **Security Analyst (Gemini API)**: When the baseline model detects a mathematical anomaly, the data is passed to the Google Gemini API to generate a clear, human-readable JSON explanation of the attack.

For a detailed visual breakdown of the architecture and data flow, see [SYSTEM_ARCHITECTURE.md](./SYSTEM_ARCHITECTURE.md).

## Features

- **Non-Blocking Architecture**: A high-performance C plugin hooks into Eclipse Mosquitto and ships data to Redis via `hiredis` in microseconds.
- **Dynamic Feature Engineering**: Extracts 14+ specific mathematical features per device, including:
  - Session Length & Message Counts
  - Topic Depth & Wildcard Usage
  - QoS, Payload Size, and Inter-Arrival Timing
- **Hybrid AI Detection**: Combines strict ML isolation forests with LLM explainability.
- **Simulated Traffic & Attacks**: Includes a built-in multithreaded Python simulator to generate normal devices (sensors, locks) and launch sophisticated MQTT attacks (Credential Replay, Wildcard Recon, Firmware Drift).
- **MySQL Storage**: Persists device drift histories, ML model baselines, and quarantine logs.

## Setup & Execution

### 1. Requirements
- Docker and Docker Compose
- Python 3.10+ (for running the simulator locally)
- A Google Gemini API Key

### 2. Configuration
Create a `.env` file in the root directory and add your API key:
```env
GEMINI_API_KEY=your_api_key_here
```

### 3. Build & Run
Spin up the entire stack (Mosquitto Broker, Redis Queue, Python Scoring Worker, MySQL Database, and Grafana):
```bash
docker-compose up -d --build
```

### 4. Run the Traffic Simulator
To train the baseline models, start generating normal traffic:
```bash
python traffic-simulator/run_simulation.py --duration 120
```

To test the anomaly detection and Gemini explainability, launch an attack simulation:
```bash
python traffic-simulator/run_simulation.py --duration 60 --attack drift
```

## Repository Structure
- `broker-plugin/`: The C plugin that hooks into Mosquitto.
- `scoring-worker/`: The Python service that consumes Redis, builds features, runs ML models, and talks to Gemini.
- `traffic-simulator/`: The test suite for generating authentic IoT baseline traffic and malicious attacks.
- `mosquitto-config/`: The ACL and broker configuration files.
