"""Trust-score fusion, graduated verdicts, and hysteresis (spec §5.4)."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum


class VerdictLevel(IntEnum):
    ALLOW = 0
    WATCH = 1
    THROTTLE = 2
    QUARANTINE = 3
    KICK = 4


@dataclass
class Verdict:
    level: VerdictLevel
    score: float
    rate: float
    reason: str


@dataclass
class ClientPolicyState:
    level: VerdictLevel = VerdictLevel.ALLOW
    consecutive_below: int = 0
    consecutive_kick_threshold: int = 0


def compute_trust_score(fsm_violation: float, drift: float, fleet_component: float, weights) -> float:
    """T = w_fsm*fsm_violation + w_drift*drift + w_fleet*fleet_component (spec §5.4)."""
    return weights.fsm * fsm_violation + weights.drift * drift + weights.fleet * fleet_component


class PolicyEngine:
    """Owns per-client hysteresis state across windows. `evaluate()` is
    meant to be called once per closed feature window per client."""

    def __init__(self, thresholds, hysteresis):
        self.thresholds = thresholds
        self.hysteresis = hysteresis
        self._state: dict[str, ClientPolicyState] = {}

    def _raw_target_level(self, trust: float) -> VerdictLevel:
        th = self.thresholds
        if trust >= th.kick:
            return VerdictLevel.KICK
        if trust >= th.quarantine:
            return VerdictLevel.QUARANTINE
        if trust >= th.throttle:
            return VerdictLevel.THROTTLE
        if trust >= th.watch:
            return VerdictLevel.WATCH
        return VerdictLevel.ALLOW

    def _lower_bound_for(self, level: VerdictLevel) -> float:
        th = self.thresholds
        return {
            VerdictLevel.WATCH: th.watch,
            VerdictLevel.THROTTLE: th.throttle,
            VerdictLevel.QUARANTINE: th.quarantine,
            VerdictLevel.KICK: th.kick,
        }.get(level, 0.0)

    def evaluate(self, client_id: str, trust: float, baseline_rate: float,
                 is_learning: bool = False, reason: str = "") -> Verdict:
        state = self._state.setdefault(client_id, ClientPolicyState())
        th = self.thresholds

        target = self._raw_target_level(trust)

        # New-client learning cap (spec §5.2): capped at WATCH while
        # learning unless trust blows past the hard ceiling.
        if is_learning and trust < th.kick_single:
            target = min(target, VerdictLevel.WATCH)

        # KICK gating (spec §5.4): a single window >= kick_single kicks
        # immediately; >= kick (but below kick_single) needs 2 consecutive
        # such windows to protect against a single noisy window.
        if target == VerdictLevel.KICK and trust < th.kick_single:
            state.consecutive_kick_threshold += 1
            if state.consecutive_kick_threshold < 2:
                target = VerdictLevel.QUARANTINE
        else:
            state.consecutive_kick_threshold = 1 if trust >= th.kick_single else 0

        if target > state.level:
            state.level = target
            state.consecutive_below = 0
        elif target < state.level:
            lower_bound = self._lower_bound_for(state.level) - self.hysteresis.margin
            if trust < lower_bound:
                state.consecutive_below += 1
            else:
                state.consecutive_below = 0
            if state.consecutive_below >= self.hysteresis.deescalate_windows:
                state.level = target
                state.consecutive_below = 0
        else:
            state.consecutive_below = 0

        rate = 0.0
        if state.level == VerdictLevel.THROTTLE:
            rate = max(1.0, baseline_rate * (1 - trust))

        return Verdict(level=state.level, score=trust, rate=rate, reason=reason)
