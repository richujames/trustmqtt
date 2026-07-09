from tmq_worker.features import FeatureWindowManager


def make_evt(ts, event, client_id="dev-1", **kw):
    e = {"v": 1, "ts": ts, "event": event, "client_id": client_id}
    e.update(kw)
    return e


def test_basic_publish_rate_and_topic_stats():
    mgr = FeatureWindowManager(window_s=60)
    for i in range(5):
        mgr.process_event(make_evt(i * 10.0, "publish", topic="a/b", qos=1, payload_len=100))

    closed = mgr.sweep_stale(now=1000.0)
    assert len(closed) == 1
    w = closed[0]
    assert w.client_id == "dev-1"
    assert w.unique_topics == 1
    assert w.qos1_ratio == 1.0
    assert w.qos0_ratio == 0.0
    assert abs(w.mean_payload_len - 100.0) < 1e-9
    assert w.msg_rate == 5 / 60.0
    assert w.byte_rate == 500 / 60.0


def test_window_rolls_over_on_time_boundary():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "publish", topic="a/1", qos=0, payload_len=10))
    mgr.process_event(make_evt(30.0, "publish", topic="a/1", qos=0, payload_len=10))
    # This event is past window_start(0) + window_s(60) -> rolls the window.
    closed = mgr.process_event(make_evt(65.0, "publish", topic="a/1", qos=0, payload_len=10))
    assert closed is not None
    assert closed.window_start == 0.0
    assert closed.msg_rate == 2 / 60.0


def test_new_topic_ratio_persists_known_topics_across_windows():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "publish", topic="a/1", qos=0, payload_len=1))
    w1 = mgr.sweep_stale(now=1000.0)[0]
    assert w1.new_topic_ratio == 1.0  # first time ever seeing a/1

    mgr.process_event(make_evt(2000.0, "publish", topic="a/1", qos=0, payload_len=1))
    mgr.process_event(make_evt(2001.0, "publish", topic="a/2", qos=0, payload_len=1))
    w2 = mgr.sweep_stale(now=3000.0)[0]
    # a/1 is known from before, a/2 is new -> 1 of 2 unique topics is new
    assert w2.unique_topics == 2
    assert abs(w2.new_topic_ratio - 0.5) < 1e-9


def test_ka_conformance_and_silent_alive_ratio():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "connect", keepalive=10))
    mgr.process_event(make_evt(5.0, "publish", topic="a/1", qos=0, payload_len=1))
    mgr.process_event(make_evt(20.0, "publish", topic="a/1", qos=0, payload_len=1))
    mgr.process_event(make_evt(30.0, "ka_gap", gap_s=16.0, keepalive=10))
    # A 3rd activity after the gap gives >=2 inter-activity gaps, so the
    # p95 calc has something to work with; this one reflects the 20s gap.
    mgr.process_event(make_evt(40.0, "publish", topic="a/1", qos=0, payload_len=1))
    closed = mgr.sweep_stale(now=1000.0)[0]
    assert closed.ka_conformance > 1.0  # observed gap (20s) exceeded keepalive (10s)
    assert abs(closed.silent_alive_ratio - (16.0 / 60.0)) < 1e-9


def test_sub_unsub_counts_and_sub_count_delta():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "subscribe", topic="a/1", sub_count=1))
    mgr.process_event(make_evt(1.0, "subscribe", topic="a/2", sub_count=2))
    mgr.process_event(make_evt(2.0, "unsubscribe", topic="a/1", sub_count=1))
    closed = mgr.sweep_stale(now=1000.0)[0]
    assert closed.sub_events == 2
    assert closed.unsub_events == 1
    assert closed.sub_count_delta == 0  # first sample 1, last sample 1


def test_empty_window_has_zeroed_ratios_not_nan():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "connect", keepalive=30))
    closed = mgr.sweep_stale(now=1000.0)[0]
    assert closed.msg_rate == 0.0
    assert closed.qos0_ratio == 0.0
    assert closed.new_topic_ratio == 0.0
    assert closed.connect_events == 1


def test_to_vector_and_as_dict_are_consistent_and_ordered():
    mgr = FeatureWindowManager(window_s=60)
    mgr.process_event(make_evt(0.0, "publish", topic="a/1", qos=2, payload_len=50))
    closed = mgr.sweep_stale(now=1000.0)[0]
    vec = closed.to_vector()
    d = closed.as_dict()
    assert len(vec) == len(d)
    assert vec[list(d.keys()).index("qos2_ratio")] == d["qos2_ratio"] == 1.0
