from tmq_worker.config import HysteresisConfig, ThresholdsConfig, WeightsConfig
from tmq_worker.policy import (
    PolicyEngine, VerdictLevel, compute_trust_score, renormalize_weights,
)

TH = ThresholdsConfig()  # watch=.30 throttle=.50 quarantine=.70 kick=.85 kick_single=.95
HY = HysteresisConfig()  # deescalate_windows=3 margin=.05


def test_compute_trust_score_weighted_sum():
    w = WeightsConfig(fsm=0.45, drift=0.35, fleet=0.20)
    t = compute_trust_score(fsm_violation=1.0, drift=0.0, fleet_component=0.0, weights=w)
    assert abs(t - 0.45) < 1e-9
    t2 = compute_trust_score(fsm_violation=0.5, drift=0.5, fleet_component=0.5, weights=w)
    assert abs(t2 - 0.5) < 1e-9


def test_adaptive_fusion_matches_static_when_all_signals_active():
    w = WeightsConfig(fsm=0.45, drift=0.35, fleet=0.20)
    active = {"fsm": True, "drift": True, "fleet": True}
    static = compute_trust_score(0.6, 0.4, 0.2, weights=w)
    adaptive = compute_trust_score(0.6, 0.4, 0.2, weights=w, active=active)
    assert abs(static - adaptive) < 1e-9


def test_adaptive_fusion_renormalizes_over_active_signals_on_cold_start():
    w = WeightsConfig(fsm=0.45, drift=0.35, fleet=0.20)
    # Drift model unfitted (cold start): drift is excluded, so a strong FSM
    # signal is no longer diluted by the 0.35*0 drift term.
    active = {"fsm": True, "drift": False, "fleet": True}
    # weights renormalize to fsm=0.45/0.65, fleet=0.20/0.65.
    w_fsm, w_drift, w_fleet = renormalize_weights(w, active)
    assert w_drift == 0.0
    assert abs(w_fsm - 0.45 / 0.65) < 1e-9
    assert abs(w_fleet - 0.20 / 0.65) < 1e-9
    static = compute_trust_score(1.0, 0.0, 0.0, weights=w)            # 0.45
    adaptive = compute_trust_score(1.0, 0.0, 0.0, weights=w, active=active)  # ~0.692
    assert adaptive > static


def test_low_trust_stays_allow():
    engine = PolicyEngine(TH, HY)
    v = engine.evaluate("dev-1", trust=0.1, baseline_rate=5.0)
    assert v.level == VerdictLevel.ALLOW
    assert v.rate == 0.0


def test_escalation_is_immediate():
    engine = PolicyEngine(TH, HY)
    engine.evaluate("dev-1", trust=0.1, baseline_rate=5.0)
    v = engine.evaluate("dev-1", trust=0.75, baseline_rate=5.0)  # jumps straight to quarantine range
    assert v.level == VerdictLevel.QUARANTINE


def test_throttle_rate_uses_baseline_and_trust():
    engine = PolicyEngine(TH, HY)
    v = engine.evaluate("dev-1", trust=0.6, baseline_rate=10.0)
    assert v.level == VerdictLevel.THROTTLE
    assert v.rate == max(1.0, 10.0 * (1 - 0.6))


def test_deescalation_requires_consecutive_windows_below_margin():
    engine = PolicyEngine(TH, HY)
    engine.evaluate("dev-1", trust=0.9, baseline_rate=5.0)  # -> escalate toward quarantine/kick region
    state_level_before = engine._state["dev-1"].level
    assert state_level_before >= VerdictLevel.QUARANTINE

    # Trust drops below ALLOW immediately, but de-escalation needs 3
    # consecutive windows below (lower_bound_for(current level) - margin).
    for i in range(2):
        v = engine.evaluate("dev-1", trust=0.05, baseline_rate=5.0)
        assert v.level == state_level_before, f"de-escalated too early on iteration {i}"
    v_final = engine.evaluate("dev-1", trust=0.05, baseline_rate=5.0)
    assert v_final.level < state_level_before


def test_kick_single_window_at_or_above_kick_single_threshold():
    engine = PolicyEngine(TH, HY)
    v = engine.evaluate("dev-1", trust=0.97, baseline_rate=5.0)
    assert v.level == VerdictLevel.KICK


def test_kick_requires_two_consecutive_windows_below_kick_single():
    engine = PolicyEngine(TH, HY)
    v1 = engine.evaluate("dev-1", trust=0.88, baseline_rate=5.0)  # >=kick(.85), <kick_single(.95)
    assert v1.level == VerdictLevel.QUARANTINE  # gated: only 1st window so far
    v2 = engine.evaluate("dev-1", trust=0.88, baseline_rate=5.0)
    assert v2.level == VerdictLevel.KICK


def test_learning_client_capped_at_watch_unless_hard_ceiling_exceeded():
    engine = PolicyEngine(TH, HY)
    v = engine.evaluate("dev-1", trust=0.9, baseline_rate=5.0, is_learning=True)
    assert v.level == VerdictLevel.WATCH  # capped despite high trust

    v2 = engine.evaluate("dev-2", trust=0.97, baseline_rate=5.0, is_learning=True)
    assert v2.level == VerdictLevel.KICK  # hard ceiling still applies during learning
