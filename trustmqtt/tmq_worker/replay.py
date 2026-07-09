"""Benchmark & evaluation harness (spec §9). Replays labeled traffic (from
a converted MQTTset/MQTT-IoT-IDS2020 CSV, or a synthetic YAML scenario) as
real MQTT traffic against the dockerized broker, then reports detection
metrics (precision/recall/F1/AUC) and TrustMQTT's enforcement differentiator
(time-to-mitigation, false-quarantine rate, benign-throughput retention).

Expected CSV schema for the dataset adapters (spec §9.1a/b) — convert
MQTTset / MQTT-IoT-IDS2020's native format to this shape once, offline;
this harness does not parse their raw PCAP/proprietary formats directly:

    timestamp,client_id,topic,qos,retain,payload_len,label
    1700000000.0,sensor-01,home/temp,1,0,32,benign
    1700000001.2,sensor-01,home/temp,1,0,4096,flood

Any `label` other than "benign" becomes one ground-truth interval per
contiguous same-label run for that client_id.
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import time
from dataclasses import dataclass

import yaml


@dataclass
class ReplayEvent:
    ts: float
    client_id: str
    topic: str
    qos: int
    retain: bool
    payload_len: int
    label: str = "benign"


@dataclass
class LabelInterval:
    client_id: str
    start_ts: float
    end_ts: float
    attack_type: str


def events_to_label_intervals(events: list[ReplayEvent]) -> list[LabelInterval]:
    """Collapses contiguous same-label runs per client into ground-truth
    attack intervals (benign runs produce no interval)."""
    intervals: list[LabelInterval] = []
    by_client: dict[str, list[ReplayEvent]] = {}
    for e in events:
        by_client.setdefault(e.client_id, []).append(e)

    for client_id, evs in by_client.items():
        evs = sorted(evs, key=lambda e: e.ts)
        cur_label = None
        start_ts = end_ts = None
        for e in evs:
            if e.label != cur_label:
                if cur_label and cur_label != "benign":
                    intervals.append(LabelInterval(client_id, start_ts, end_ts, cur_label))
                cur_label = e.label
                start_ts = e.ts
            end_ts = e.ts
        if cur_label and cur_label != "benign":
            intervals.append(LabelInterval(client_id, start_ts, end_ts, cur_label))
    return intervals


def load_csv_dataset(path: str) -> list[ReplayEvent]:
    events = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            events.append(ReplayEvent(
                ts=float(row["timestamp"]),
                client_id=row["client_id"],
                topic=row["topic"],
                qos=int(row.get("qos") or 0),
                retain=(row.get("retain") or "0") in ("1", "true", "True"),
                payload_len=int(row.get("payload_len") or 0),
                label=row.get("label") or "benign",
            ))
    events.sort(key=lambda e: e.ts)
    return events


# --- Synthetic scenario adapter (spec §9.1c) -----------------------------

def load_synthetic_scenario(path: str) -> list[ReplayEvent]:
    """YAML scenario schema:

    clients: 30
    duration_s: 600
    baseline_rate_hz: 1.0
    topic_template: "fleet/{client_id}/temp"
    attacks:
      - type: credential_reuse | topic_scope_expansion | slow_rate_escalation | coordinated_drift
        start_s: 300
        duration_s: 60
        clients: [0, 1, 2]   # indices into the synthetic fleet; omit for "all"
    """
    with open(path, "r", encoding="utf-8") as f:
        spec = yaml.safe_load(f)

    n_clients = spec["clients"]
    duration_s = spec["duration_s"]
    rate_hz = spec.get("baseline_rate_hz", 1.0)
    topic_template = spec.get("topic_template", "fleet/{client_id}/data")
    client_ids = [f"sim-{i:03d}" for i in range(n_clients)]

    events: list[ReplayEvent] = []
    period = 1.0 / rate_hz
    for cid in client_ids:
        t = 0.0
        while t < duration_s:
            events.append(ReplayEvent(ts=t, client_id=cid, topic=topic_template.format(client_id=cid),
                                       qos=1, retain=False, payload_len=32, label="benign"))
            t += period

    for attack in spec.get("attacks", []):
        attack_type = attack["type"]
        start_s = attack["start_s"]
        end_s = start_s + attack["duration_s"]
        indices = attack.get("clients")
        targets = [client_ids[i] for i in indices] if indices is not None else client_ids
        for cid in targets:
            _apply_attack(events, cid, attack_type, start_s, end_s, period)

    events.sort(key=lambda e: e.ts)
    return events


def _apply_attack(events: list[ReplayEvent], client_id: str, attack_type: str,
                   start_s: float, end_s: float, base_period: float):
    # Every attack type relabels this client's pre-existing baseline events
    # within [start_s, end_s) too, so the resulting ground-truth interval is
    # one clean continuous span rather than flickering between "benign" and
    # the attack label as the two event streams interleave by timestamp.
    for e in events:
        if e.client_id == client_id and start_s <= e.ts < end_s:
            e.label = attack_type

    if attack_type == "credential_reuse":
        t = start_s
        while t < end_s:
            events.append(ReplayEvent(ts=t, client_id=client_id, topic=f"fleet/{client_id}/admin",
                                       qos=2, retain=False, payload_len=256, label=attack_type))
            t += base_period
    elif attack_type == "topic_scope_expansion":
        t, i = start_s, 0
        while t < end_s:
            events.append(ReplayEvent(ts=t, client_id=client_id, topic=f"fleet/{client_id}/scan/{i}",
                                       qos=0, retain=False, payload_len=8, label=attack_type))
            t += base_period / 4
            i += 1
    elif attack_type == "slow_rate_escalation":
        t = start_s
        while t < end_s:
            steps = max(1, int((t - start_s) / max(base_period, 1e-6)))
            rate_multiplier = 1.0 + steps * 0.05
            events.append(ReplayEvent(ts=t, client_id=client_id, topic=f"fleet/{client_id}/data",
                                       qos=1, retain=False, payload_len=32, label=attack_type))
            t += base_period / rate_multiplier
    elif attack_type == "coordinated_drift":
        t = start_s
        while t < end_s:
            events.append(ReplayEvent(ts=t, client_id=client_id, topic=f"fleet/{client_id}/data",
                                       qos=1, retain=False, payload_len=4096, label=attack_type))
            t += base_period / 3
    else:
        raise ValueError(f"unknown attack type: {attack_type}")


# --- MQTT replay driver (needs a live broker; integration-only) ---------

def replay_via_mqtt(events: list[ReplayEvent], host: str, port: int, speed: float = 1.0):
    """Publishes `events` against a real broker, preserving inter-arrival
    timing scaled by `speed`. Opens one real MQTT connection per distinct
    `client_id` in the replay (each connects up front) so the broker/plugin
    sees genuinely separate client identities — TrustMQTT scores behavior
    per client_id, so funneling every event through a single shared
    connection would make every replayed device invisible as itself.
    Requires paho-mqtt and a reachable broker — this is the one function in
    this module that cannot be unit tested without live infrastructure."""
    import paho.mqtt.client as mqtt

    if not events:
        return

    client_ids = sorted({e.client_id for e in events})
    clients = {}
    for cid in client_ids:
        c = mqtt.Client(client_id=cid, clean_session=True)
        c.connect(host, port)
        c.loop_start()
        clients[cid] = c

    try:
        t0_wall = time.time()
        t0_sim = events[0].ts
        for e in events:
            target_wall = t0_wall + (e.ts - t0_sim) / speed
            sleep_s = target_wall - time.time()
            if sleep_s > 0:
                time.sleep(sleep_s)
            clients[e.client_id].publish(e.topic, payload=b"x" * e.payload_len, qos=e.qos, retain=e.retain)
    finally:
        for c in clients.values():
            c.loop_stop()
            c.disconnect()


# --- Metrics (spec §9.2) -------------------------------------------------

@dataclass
class VerdictSample:
    ts: float
    client_id: str
    level: int
    trust: float


def _is_labeled_attack(client_id: str, ts: float, intervals: list[LabelInterval]) -> bool:
    return any(iv.client_id == client_id and iv.start_ts <= ts <= iv.end_ts for iv in intervals)


def _roc_auc(y_true: list[int], y_score: list[float]) -> float:
    """Mann-Whitney-U based AUC (no sklearn dependency needed for this).
    Returns 0.5 (chance) if either class is empty."""
    pos = [s for y, s in zip(y_true, y_score) if y == 1]
    neg = [s for y, s in zip(y_true, y_score) if y == 0]
    if not pos or not neg:
        return 0.5

    combined = sorted(pos + neg)
    ranks: dict[float, float] = {}
    i = 0
    while i < len(combined):
        j = i
        while j < len(combined) and combined[j] == combined[i]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[combined[k]] = avg_rank
        i = j

    rank_sum_pos = sum(ranks[s] for s in pos)
    n_pos, n_neg = len(pos), len(neg)
    return (rank_sum_pos - n_pos * (n_pos + 1) / 2.0) / (n_pos * n_neg)


def compute_detection_metrics(samples: list[VerdictSample], intervals: list[LabelInterval],
                               predicted_positive_level: int = 1) -> dict:
    """Precision/recall/F1 (verdict level >= predicted_positive_level as
    the predicted-positive rule) plus ROC-AUC of trust score vs. label."""
    y_true, y_score = [], []
    tp = fp = fn = tn = 0
    for s in samples:
        actual = _is_labeled_attack(s.client_id, s.ts, intervals)
        predicted = s.level >= predicted_positive_level
        y_true.append(1 if actual else 0)
        y_score.append(s.trust)
        if predicted and actual:
            tp += 1
        elif predicted and not actual:
            fp += 1
        elif not predicted and actual:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "precision": precision, "recall": recall, "f1": f1,
        "roc_auc": _roc_auc(y_true, y_score),
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
    }


def compute_enforcement_metrics(samples: list[VerdictSample], intervals: list[LabelInterval],
                                 mitigation_level: int = 2, quarantine_level: int = 3) -> dict:
    """Time-to-mitigation, false-quarantine rate, benign-throughput
    retention — TrustMQTT's enforcement differentiator (spec §9.2)."""
    times_to_mitigation = []
    for iv in intervals:
        client_samples = sorted((s for s in samples if s.client_id == iv.client_id), key=lambda s: s.ts)
        for s in client_samples:
            if s.ts >= iv.start_ts and s.level >= mitigation_level:
                times_to_mitigation.append(s.ts - iv.start_ts)
                break

    benign_samples = [s for s in samples if not _is_labeled_attack(s.client_id, s.ts, intervals)]
    false_quarantine = sum(1 for s in benign_samples if s.level >= quarantine_level)
    false_quarantine_rate = (false_quarantine / len(benign_samples)) if benign_samples else 0.0
    benign_allowed = sum(1 for s in benign_samples if s.level < quarantine_level)
    benign_throughput_retention = (benign_allowed / len(benign_samples)) if benign_samples else 1.0

    return {
        "mean_time_to_mitigation_s": (sum(times_to_mitigation) / len(times_to_mitigation))
        if times_to_mitigation else None,
        "attacks_mitigated": len(times_to_mitigation),
        "attacks_total": len(intervals),
        "false_quarantine_rate": false_quarantine_rate,
        "benign_throughput_retention": benign_throughput_retention,
    }


def collect_recorded_verdicts(session_factory, client_ids: set, start_wall: float, end_wall: float) -> list:
    """Pulls the VerdictHistory rows the live worker wrote during
    [start_wall, end_wall] (unix seconds) for the given client_ids, and
    converts each to a VerdictSample whose .ts is seconds-since-replay-start
    — the same axis the synthetic/dataset event timestamps use, so it lines
    up directly with the LabelInterval objects from events_to_label_intervals.
    Imports tmq_worker.storage locally so this module stays importable (and
    its pure functions testable) without a Postgres driver available."""
    from tmq_worker.storage import Client, VerdictHistory

    session = session_factory()
    try:
        start_dt = datetime.datetime.fromtimestamp(start_wall, datetime.timezone.utc).replace(tzinfo=None)
        end_dt = datetime.datetime.fromtimestamp(end_wall, datetime.timezone.utc).replace(tzinfo=None)
        rows = (
            session.query(VerdictHistory, Client.client_id)
            .join(Client, Client.id == VerdictHistory.client_id)
            .filter(Client.client_id.in_(client_ids))
            .filter(VerdictHistory.ts >= start_dt, VerdictHistory.ts <= end_dt)
            .all()
        )
        samples = []
        for vh, cid in rows:
            wall_ts = vh.ts.replace(tzinfo=datetime.timezone.utc).timestamp()
            samples.append(VerdictSample(ts=wall_ts - start_wall, client_id=cid, level=vh.level, trust=vh.score))
        return samples
    finally:
        session.close()


def write_report(detection: dict, enforcement: dict, out_dir: str, name: str = "eval_report") -> tuple[str, str]:
    os.makedirs(out_dir, exist_ok=True)
    json_path = os.path.join(out_dir, f"{name}.json")
    md_path = os.path.join(out_dir, f"{name}.md")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({"detection": detection, "enforcement": enforcement}, f, indent=2)

    lines = ["# TrustMQTT evaluation report", "", "## Detection", "", "| metric | value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in detection.items()]
    lines += ["", "## Enforcement", "", "| metric | value |", "|---|---|"]
    lines += [f"| {k} | {v} |" for k, v in enforcement.items()]
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    return json_path, md_path


def main():
    parser = argparse.ArgumentParser(description="TrustMQTT benchmark/eval harness")
    parser.add_argument("--scenario", help="Path to a synthetic scenario YAML (spec §9.1c)")
    parser.add_argument("--dataset", help="Path to a converted MQTTset/MQTT-IoT-IDS2020 CSV (spec §9.1a/b)")
    parser.add_argument("--broker-host", default="localhost")
    parser.add_argument("--broker-port", type=int, default=1883)
    parser.add_argument("--speed", type=float, default=1.0)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--results-dir", default="eval/results")
    args = parser.parse_args()

    if args.scenario:
        events = load_synthetic_scenario(args.scenario)
    elif args.dataset:
        events = load_csv_dataset(args.dataset)
    else:
        parser.error("one of --scenario or --dataset is required")

    intervals = events_to_label_intervals(events)

    start_wall = time.time()
    replay_via_mqtt(events, args.broker_host, args.broker_port, speed=args.speed)
    end_wall = time.time()

    print(f"Replayed {len(events)} events across {len(intervals)} labeled attack interval(s).")

    if not args.report:
        return

    from tmq_worker.config import load_config
    from tmq_worker.storage import get_engine, get_sessionmaker

    config = load_config()
    # The live worker's tumbling windows close up to window_s + a grace
    # period after the last event for a client (spec §4.3); wait for that
    # before querying, or the tail of the replay won't have verdicts yet.
    wait_s = config.window_s + 15
    print(f"Waiting {wait_s:.0f}s for the worker to close the last feature window and write verdicts...")
    time.sleep(wait_s)

    session_factory = get_sessionmaker(get_engine(config.database_url))
    client_ids = {e.client_id for e in events}
    samples = collect_recorded_verdicts(session_factory, client_ids, start_wall, end_wall + wait_s)

    if not samples:
        print("No recorded verdicts found for this replay's clients in this time range — "
              "is the tmq-worker service running and pointed at the same broker/database?")
        return

    detection = compute_detection_metrics(samples, intervals)
    enforcement = compute_enforcement_metrics(samples, intervals)
    json_path, md_path = write_report(detection, enforcement, args.results_dir)
    print(f"Wrote {json_path} and {md_path}")
    print(json.dumps({"detection": detection, "enforcement": enforcement}, indent=2))


if __name__ == "__main__":
    main()
