import csv
import datetime

from tmq_worker.replay import (
    LabelInterval, ReplayEvent, VerdictSample, _roc_auc,
    collect_recorded_verdicts, compute_detection_metrics,
    compute_enforcement_metrics, events_to_label_intervals, load_csv_dataset,
    load_synthetic_scenario, write_report,
)
from tmq_worker.storage import (
    VerdictHistory, get_engine, get_or_create_client, get_sessionmaker, init_db,
)


def test_events_to_label_intervals_collapses_contiguous_runs():
    events = [
        ReplayEvent(0.0, "dev-1", "a/b", 0, False, 10, "benign"),
        ReplayEvent(1.0, "dev-1", "a/b", 0, False, 10, "benign"),
        ReplayEvent(2.0, "dev-1", "a/b", 0, False, 10, "flood"),
        ReplayEvent(3.0, "dev-1", "a/b", 0, False, 10, "flood"),
        ReplayEvent(4.0, "dev-1", "a/b", 0, False, 10, "benign"),
    ]
    intervals = events_to_label_intervals(events)
    assert len(intervals) == 1
    assert intervals[0] == LabelInterval("dev-1", 2.0, 3.0, "flood")


def test_events_to_label_intervals_all_benign_yields_no_intervals():
    events = [ReplayEvent(float(i), "dev-1", "a/b", 0, False, 10, "benign") for i in range(5)]
    assert events_to_label_intervals(events) == []


def test_load_csv_dataset_parses_expected_schema(tmp_path):
    path = tmp_path / "data.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "client_id", "topic", "qos", "retain", "payload_len", "label"])
        w.writerow(["1700000000.0", "sensor-01", "home/temp", "1", "0", "32", "benign"])
        w.writerow(["1700000001.2", "sensor-01", "home/temp", "1", "0", "4096", "flood"])

    events = load_csv_dataset(str(path))
    assert len(events) == 2
    assert events[0].client_id == "sensor-01"
    assert events[0].label == "benign"
    assert events[1].label == "flood"
    assert events[1].payload_len == 4096


def test_load_synthetic_scenario_generates_baseline_and_attack(tmp_path):
    scenario = tmp_path / "scenario.yaml"
    scenario.write_text("""
clients: 3
duration_s: 10
baseline_rate_hz: 2.0
attacks:
  - type: topic_scope_expansion
    start_s: 5
    duration_s: 2
    clients: [0]
""", encoding="utf-8")

    events = load_synthetic_scenario(str(scenario))
    client_ids = {e.client_id for e in events}
    assert client_ids == {"sim-000", "sim-001", "sim-002"}

    attack_events = [e for e in events if e.label == "topic_scope_expansion"]
    assert len(attack_events) > 0
    assert all(e.client_id == "sim-000" for e in attack_events)
    assert all(5.0 <= e.ts < 7.0 for e in attack_events)


def test_unknown_attack_type_raises(tmp_path):
    scenario = tmp_path / "bad.yaml"
    scenario.write_text("""
clients: 1
duration_s: 5
attacks:
  - type: not_a_real_attack
    start_s: 1
    duration_s: 1
""", encoding="utf-8")
    import pytest
    with pytest.raises(ValueError):
        load_synthetic_scenario(str(scenario))


def test_roc_auc_perfect_separation_is_one():
    y_true = [0, 0, 0, 1, 1, 1]
    y_score = [0.1, 0.2, 0.3, 0.7, 0.8, 0.9]
    assert _roc_auc(y_true, y_score) == 1.0


def test_roc_auc_random_scores_is_near_half():
    y_true = [0, 1, 0, 1]
    y_score = [0.5, 0.5, 0.5, 0.5]  # all tied -> exactly 0.5
    assert _roc_auc(y_true, y_score) == 0.5


def test_roc_auc_empty_class_defaults_to_half():
    assert _roc_auc([0, 0, 0], [0.1, 0.2, 0.3]) == 0.5


def test_compute_detection_metrics_perfect_detector():
    intervals = [LabelInterval("dev-1", 10.0, 20.0, "flood")]
    samples = [
        VerdictSample(ts=0.0, client_id="dev-1", level=0, trust=0.1),
        VerdictSample(ts=15.0, client_id="dev-1", level=3, trust=0.9),
        VerdictSample(ts=30.0, client_id="dev-1", level=0, trust=0.1),
    ]
    metrics = compute_detection_metrics(samples, intervals, predicted_positive_level=1)
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 1.0
    assert metrics["f1"] == 1.0
    assert metrics["roc_auc"] == 1.0


def test_compute_detection_metrics_missed_attack_hurts_recall():
    intervals = [LabelInterval("dev-1", 10.0, 20.0, "flood")]
    samples = [
        VerdictSample(ts=15.0, client_id="dev-1", level=0, trust=0.1),  # missed it
    ]
    metrics = compute_detection_metrics(samples, intervals, predicted_positive_level=1)
    assert metrics["recall"] == 0.0
    assert metrics["fn"] == 1


def test_compute_enforcement_metrics_time_to_mitigation_and_false_quarantine():
    intervals = [LabelInterval("dev-1", 10.0, 20.0, "flood")]
    samples = [
        VerdictSample(ts=0.0, client_id="dev-1", level=0, trust=0.1),   # benign, allowed
        VerdictSample(ts=5.0, client_id="dev-2", level=3, trust=0.9),   # benign but wrongly quarantined
        VerdictSample(ts=12.0, client_id="dev-1", level=1, trust=0.4),  # attack, not yet mitigated
        VerdictSample(ts=14.0, client_id="dev-1", level=2, trust=0.6),  # attack, mitigated here (+4s)
    ]
    metrics = compute_enforcement_metrics(samples, intervals, mitigation_level=2, quarantine_level=3)
    assert metrics["mean_time_to_mitigation_s"] == 4.0
    assert metrics["attacks_mitigated"] == 1
    assert metrics["attacks_total"] == 1
    # 2 benign samples total, 1 wrongly quarantined -> 0.5
    assert metrics["false_quarantine_rate"] == 0.5
    assert metrics["benign_throughput_retention"] == 0.5


def test_compute_enforcement_metrics_no_attacks_mitigated_is_none():
    intervals = [LabelInterval("dev-1", 10.0, 20.0, "flood")]
    samples = [VerdictSample(ts=12.0, client_id="dev-1", level=0, trust=0.1)]
    metrics = compute_enforcement_metrics(samples, intervals)
    assert metrics["mean_time_to_mitigation_s"] is None
    assert metrics["attacks_mitigated"] == 0


def test_write_report_produces_json_and_markdown(tmp_path):
    json_path, md_path = write_report(
        {"precision": 1.0}, {"attacks_mitigated": 1}, str(tmp_path), name="test_report",
    )
    import json as jsonlib
    data = jsonlib.loads(open(json_path, encoding="utf-8").read())
    assert data["detection"]["precision"] == 1.0
    md = open(md_path, encoding="utf-8").read()
    assert "TrustMQTT evaluation report" in md
    assert "attacks_mitigated" in md


def test_collect_recorded_verdicts_maps_wall_clock_to_replay_relative_seconds():
    engine = get_engine("sqlite:///:memory:")
    init_db(engine)
    session_factory = get_sessionmaker(engine)
    session = session_factory()

    client = get_or_create_client(session, "sim-000")
    other_client = get_or_create_client(session, "sim-001")
    session.commit()

    start_wall = 1_700_000_000.0
    end_wall = start_wall + 60.0

    def ts_at(offset_s):
        return datetime.datetime.fromtimestamp(start_wall + offset_s, datetime.timezone.utc).replace(tzinfo=None)

    session.add(VerdictHistory(client_id=client.id, ts=ts_at(5.0), level=1, score=0.4, reason="r"))
    session.add(VerdictHistory(client_id=client.id, ts=ts_at(30.0), level=2, score=0.6, reason="r"))
    # Outside the replay's time window -> must not appear in results.
    session.add(VerdictHistory(client_id=client.id, ts=ts_at(-100.0), level=0, score=0.0, reason="r"))
    # Belongs to a client not in this replay's client_ids set -> excluded.
    session.add(VerdictHistory(client_id=other_client.id, ts=ts_at(10.0), level=3, score=0.9, reason="r"))
    session.commit()
    session.close()

    samples = collect_recorded_verdicts(session_factory, {"sim-000"}, start_wall, end_wall)

    assert len(samples) == 2
    by_ts = sorted(samples, key=lambda s: s.ts)
    assert by_ts[0].client_id == "sim-000"
    assert abs(by_ts[0].ts - 5.0) < 1.0
    assert by_ts[0].level == 1
    assert abs(by_ts[1].ts - 30.0) < 1.0
    assert by_ts[1].level == 2
