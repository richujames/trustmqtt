# TrustMQTT System Architecture

Here is the complete architectural layout of how the components we have built (and are building) interact to create a continuous behavioral identity verification system.

## High-Level Component Flow

This diagram illustrates the physical layers of the application and how data moves from the simulated IoT edge, through the C plugin, into the Python worker, and finally out to the database and dashboard.

```mermaid
graph TD
    subgraph Traffic Simulator
        ND[Normal Devices<br>Temp/Lock/Motion]
        AT[Attack Scripts<br>Replay/Drift/Recon]
    end

    subgraph Broker Layer
        MQ[Mosquitto Broker]
        CP[C Plugin<br>trustmqtt_plugin.so]
        MQ -- Event Hooks --> CP
    end

    subgraph Message Queue
        RD[(Redis)]
    end

    subgraph Python Worker Layer
        QC[Queue Consumer]
        FE[Feature Engineering<br>Vectors]
        ML[Scikit-Learn<br>Baseline Models]
        GM[Gemini Integration]
        PE[Policy Engine]
    end

    subgraph Storage & Viz Layer
        DB[(MySQL Database)]
        GF[Grafana Dashboard]
    end
    
    subgraph External
        API[Google Gemini API]
    end

    %% Flow
    ND -- MQTT TCP --> MQ
    AT -- MQTT TCP --> MQ
    
    CP -- LPUSH JSON --> RD
    
    RD -- BRPOP JSON --> QC
    QC --> FE
    
    FE -- Feature Vectors --> ML
    ML -- Fetch/Update Baseline --> DB
    
    ML -- If Anomaly Detected --> GM
    GM -- Context Prompt --> API
    API -- Explanation --> GM
    
    GM --> PE
    ML -- Normal Score --> PE
    
    PE -- Save Logs & Scores --> DB
    PE -- Enforcement Actions --> MQ
    
    GF -- Query --> DB
```

## Execution Sequence

This sequence diagram illustrates exactly what happens when an MQTT message arrives, demonstrating how the system remains non-blocking at the broker level while performing heavy machine learning tasks asynchronously.

```mermaid
sequenceDiagram
    participant IoT as IoT Devices (Simulator)
    participant Broker as Mosquitto Broker
    participant Plugin as C Plugin
    participant Redis as Redis Queue
    participant Worker as Python Worker (FE & ML)
    participant Gemini as Gemini API
    participant DB as MySQL Database

    IoT->>Broker: CONNECT / PUBLISH
    Broker->>Plugin: Trigger Event Hooks (C structs)
    Plugin->>Redis: Extract Headers & LPUSH JSON (Instant)
    Broker-->>IoT: ACK (Non-blocking)
    
    Redis-->>Worker: BRPOP JSON Event
    Worker->>Worker: Feature Engineering (Session/Message Vectors)
    Worker->>DB: Fetch Scikit-Learn Baseline Model
    Worker->>Worker: IsolationForest.score(vector)
    
    alt Anomaly Detected
        Worker->>Gemini: Send Vector + Baseline Context
        Gemini-->>Worker: Return JSON Reason (e.g. "Payload Spike")
        Worker->>DB: Log Action & Explanation
        Worker->>Broker: Execute Policy Ban (Disconnect)
    else Normal Traffic
        Worker->>DB: Log Baseline Score
    end
```

## Component Roles

1. **Traffic Simulator**: Feeds authentic, multi-threaded traffic into the broker. It generates clean data to train the baseline, and malicious data to test the anomaly detection.
2. **Mosquitto & C Plugin**: Sits at the network edge. It intercepts raw MQTT headers in microseconds using C, and pushes them to Redis so the broker doesn't freeze.
3. **Queue Consumer**: A Python script continuously polling Redis. It catches the JSON and routes it.
4. **Feature Engineering**: Converts raw MQTT events into numerical arrays (e.g., QoS 1 becomes `1.0`).
5. **Scikit-Learn (The Math)**: Mathematically compares the current numerical array against the historical baseline to generate a strict anomaly score (0.0 to 1.0).
6. **Gemini API (The Analyst)**: If the mathematical score is too high, Gemini acts as a human analyst to explain *why* the numbers changed.
7. **MySQL & Grafana**: Persists the baseline data and visualizes the attack logs.
