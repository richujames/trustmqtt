# Evaluation datasets

`tmq_worker/replay.py`'s `load_csv_dataset()` reads a single flat CSV — it does not parse MQTTset's or MQTT-IoT-IDS2020's native formats directly. Convert either dataset to this shape once, offline, and drop the result here:

```csv
timestamp,client_id,topic,qos,retain,payload_len,label
1700000000.0,sensor-01,home/temp,1,0,32,benign
1700000001.2,sensor-01,home/temp,1,0,4096,flood
```

- `timestamp`: unix seconds (float), ascending overall (per-client ordering is re-sorted by the loader anyway).
- `client_id`: the device/session identifier.
- `topic`, `qos`, `retain`, `payload_len`: resolved-semantic fields matching the plugin's own event schema (`docs/SPEC.md` §4.1) — this is deliberate, so replayed dataset traffic and live plugin traffic produce directly comparable feature vectors.
- `label`: `benign` for normal traffic; anything else is treated as an attack type and becomes one ground-truth interval per contiguous same-label run for that `client_id` (see `events_to_label_intervals` in `tmq_worker/replay.py`).

## Getting the source datasets

- **MQTTset**: published by the University of Bologna's IoT/MQTT security research group; distributed as pcap/csv captures of both normal and attack MQTT traffic (flooding, malformed messages, slowite, brute force, etc.).
- **MQTT-IoT-IDS2020**: published alongside the accompanying paper; distributed as pcap/csv captures across normal and several attack scenarios (scan, sparta brute-force, MQTT publish flood).

Both are third-party research datasets — check their respective licenses before redistributing converted copies. Write one small conversion script per dataset (not included here, since it depends on exactly which release/format you pull) that maps their columns onto the schema above, then run:

```bash
python -m tmq_worker.replay --dataset eval/datasets/mqttset_converted.csv --report
```
