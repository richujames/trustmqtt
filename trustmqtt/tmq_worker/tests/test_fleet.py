from tmq_worker.features import FEATURE_NAMES
from tmq_worker.fleet import FleetBaseline


def base_features(**overrides):
    d = {name: 0.0 for name in FEATURE_NAMES}
    d["msg_rate"] = 1.0
    d.update(overrides)
    return d


def test_single_client_component_is_bounded_and_small_when_normal():
    fb = FleetBaseline(coordinated_fraction=0.2, z_trigger=2.0)
    # Feed enough "normal" samples to build up a baseline with nonzero std.
    for i in range(30):
        fb.observe("dev-1", base_features(msg_rate=1.0 + (0.01 * (i % 3))), now=0.0)
    component, alarm = fb.observe("dev-1", base_features(msg_rate=1.0), now=0.0)
    assert alarm is False
    assert 0.0 <= component <= 1.0


def test_coordinated_spike_across_many_clients_raises_alarm():
    fb = FleetBaseline(coordinated_fraction=0.2, z_trigger=2.0, window_s=60.0)
    clients = [f"dev-{i}" for i in range(20)]
    # Build a tight baseline for msg_rate across the fleet.
    for _ in range(20):
        for cid in clients:
            fb.observe(cid, base_features(msg_rate=1.0), now=0.0)

    # Now >20% of the fleet spikes msg_rate together, same window.
    fired = []
    for cid in clients[:6]:  # 6/20 = 30% > 20% threshold
        _component, alarm = fb.observe(cid, base_features(msg_rate=50.0), now=0.0)
        fired.append(alarm)

    assert any(fired), "expected the coordinated-drift alarm to fire for at least one affected client"


def test_uncoordinated_single_outlier_does_not_raise_alarm():
    fb = FleetBaseline(coordinated_fraction=0.2, z_trigger=2.0, window_s=60.0)
    clients = [f"dev-{i}" for i in range(20)]
    for _ in range(20):
        for cid in clients:
            fb.observe(cid, base_features(msg_rate=1.0), now=0.0)

    # Only a single client (5% << 20%) spikes.
    _component, alarm = fb.observe("dev-0", base_features(msg_rate=50.0), now=0.0)
    assert alarm is False


def test_window_reset_clears_zscore_buffer_after_window_s():
    fb = FleetBaseline(coordinated_fraction=0.2, z_trigger=2.0, window_s=10.0)
    fb.observe("dev-1", base_features(msg_rate=1.0), now=0.0)
    assert fb._window_start == 0.0
    fb.observe("dev-1", base_features(msg_rate=1.0), now=5.0)
    assert len(fb._window_zscores["msg_rate"]) == 2
    # Past the window boundary -> buffer resets.
    fb.observe("dev-1", base_features(msg_rate=1.0), now=15.0)
    assert len(fb._window_zscores["msg_rate"]) == 1
