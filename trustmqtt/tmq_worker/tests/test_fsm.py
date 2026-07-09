from tmq_worker.fsm import (
    BehavioralContractEngine, ClientFSM, build_fingerprint_doc,
    compute_stability, js_divergence, topic_class,
)

REGEX = r"^[a-z]*\d+$"


def make_evt(ts, event, client_id="dev-1", **kw):
    e = {"ts": ts, "event": event, "client_id": client_id}
    e.update(kw)
    return e


def test_topic_class_normalizes_numeric_suffix_segments_only():
    assert topic_class("plant-a/line2/temp", REGEX) == "plant-a/+/temp"
    assert topic_class("plant-a/temp", REGEX) == "plant-a/temp"
    assert topic_class("sensor7/status", REGEX) == "+/status"


def test_new_client_starts_in_learning_mode():
    bce = BehavioralContractEngine(REGEX, learning_max_events=2000, learning_max_hours=24)
    assert bce.is_learning("dev-1") is True
    bce.observe(make_evt(0.0, "connect"))
    assert bce.is_learning("dev-1") is True


def test_learning_completes_after_max_events():
    bce = BehavioralContractEngine(REGEX, learning_max_events=3, learning_max_hours=24)
    bce.observe(make_evt(0.0, "connect"))
    bce.observe(make_evt(1.0, "publish", topic="a/1", qos=0))
    assert bce.is_learning("dev-1") is True
    bce.observe(make_evt(2.0, "publish", topic="a/1", qos=0))
    assert bce.is_learning("dev-1") is False


def test_repeated_transition_becomes_low_violation_low_novelty():
    fsm = ClientFSM("dev-1", REGEX, learning_max_events=1000, learning_max_hours=24)
    # Train CONNECT -> PUB(a/+,0) many times.
    fsm.observe({"ts": 0.0, "event": "connect"})
    for i in range(50):
        fsm.observe({"ts": float(i + 1), "event": "publish", "topic": "a/1", "qos": 0})
        fsm.observe({"ts": float(i + 1) + 0.5, "event": "connect"})
    # Now the learned P(PUB(a/+,0) | CONNECT) should be high -> low violation.
    viol = fsm.observe({"ts": 100.0, "event": "publish", "topic": "a/1", "qos": 0})
    assert viol is not None
    assert viol < 0.3


def test_novel_transition_has_high_violation_and_appears_in_diff():
    bce = BehavioralContractEngine(REGEX, learning_max_events=10000, learning_max_hours=24)
    # Establish a strong habit over a somewhat realistic, multi-symbol cycle
    # so the learned vocabulary is large enough that Laplace smoothing
    # actually pushes an unseen transition's probability below the 0.01
    # novelty threshold (a 2-symbol toy vocabulary wouldn't).
    bce.observe(make_evt(0.0, "connect"))
    t = 1.0
    for _ in range(300):
        bce.observe(make_evt(t, "publish", topic="sensors/status", qos=0)); t += 1
        bce.observe(make_evt(t, "publish", topic="sensors/temp", qos=0)); t += 1
        bce.observe(make_evt(t, "subscribe", topic="sensors/cmd", sub_count=1)); t += 1
    score_before, _ = bce.close_window("dev-1")

    # A completely novel transition: subscribe to a secrets-looking topic,
    # something never seen anywhere in training.
    bce.observe(make_evt(t + 100.0, "subscribe", topic="admin/keys", sub_count=1))
    score_after, diff = bce.close_window("dev-1")

    assert score_after > score_before
    assert any(d["to"].startswith("SUB(") for d in diff)


def test_serialize_reports_states_and_top_transitions():
    bce = BehavioralContractEngine(REGEX, learning_max_events=1000, learning_max_hours=24)
    bce.observe(make_evt(0.0, "connect"))
    bce.observe(make_evt(1.0, "publish", topic="a/1", qos=1))
    doc = bce.serialize("dev-1")
    assert doc["client_id"] == "dev-1"
    assert "CONNECT" in doc["states"]
    assert any(t[0] == "CONNECT" for t in doc["top_transitions"])


def test_close_window_resets_buffers():
    bce = BehavioralContractEngine(REGEX, learning_max_events=1000, learning_max_hours=24)
    bce.observe(make_evt(0.0, "connect"))
    bce.observe(make_evt(1.0, "publish", topic="a/1", qos=0))
    score1, _ = bce.close_window("dev-1")
    # Second close with no new events in between should be a clean zero.
    score2, diff2 = bce.close_window("dev-1")
    assert score2 == 0.0
    assert diff2 == []


def test_js_divergence_zero_for_identical_snapshots_positive_for_different():
    a = {"CONNECT->PUB": 0.5, "PUB->CONNECT": 0.5}
    assert js_divergence(a, a) == 0.0
    b = {"CONNECT->SUB": 0.5, "SUB->CONNECT": 0.5}
    assert js_divergence(a, b) > 0.0
    assert js_divergence({}, {}) == 0.0


def test_compute_stability_defaults_to_one_with_fewer_than_two_snapshots():
    assert compute_stability([]) == 1.0
    assert compute_stability([{"a": 1.0}]) == 1.0


def test_compute_stability_is_lower_for_a_shifting_fsm():
    stable_snapshots = [{"CONNECT->PUB": 1.0} for _ in range(3)]
    shifting_snapshots = [
        {"CONNECT->PUB": 1.0},
        {"CONNECT->SUB": 1.0},
        {"CONNECT->PUB": 0.2, "CONNECT->SUB": 0.8},
    ]
    assert compute_stability(stable_snapshots) == 1.0
    assert compute_stability(shifting_snapshots) < compute_stability(stable_snapshots)


def test_build_fingerprint_doc_shape():
    fsm = ClientFSM("dev-1", REGEX, learning_max_events=1000, learning_max_hours=24)
    fsm.observe({"ts": 0.0, "event": "connect"})
    fsm.observe({"ts": 3600.0, "event": "publish", "topic": "a/1", "qos": 0})
    snap1 = fsm.snapshot()
    fsm.observe({"ts": 7200.0, "event": "connect"})
    snap2 = fsm.snapshot()

    doc = build_fingerprint_doc(fsm, feature_baseline={"msg_rate": {"med": 1.0, "iqr": 0.2}},
                                 daily_snapshots=[snap1, snap2])
    assert doc["client_id"] == "dev-1"
    assert doc["learned_over"]["events"] == 3
    assert doc["learned_over"]["hours"] == 2.0
    assert "feature_baseline" in doc
    assert 0.0 <= doc["stability"] <= 1.0
