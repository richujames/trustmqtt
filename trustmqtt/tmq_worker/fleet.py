"""Fleet-level drift baseline and coordinated-compromise detection (spec
§5.5) — catches synchronized behavior shifts across many clients that
per-client scoring alone would miss (e.g. botnet-style compromise).
"""
from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

from tmq_worker.features import FEATURE_NAMES


@dataclass
class _RunningStat:
    """Welford's online mean/variance, one per feature, across the whole
    fleet (not per-client) — spec's "running mean/std of each feature
    across all active clients" (`tmq:fleet:baseline`)."""
    n: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, x: float):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        delta2 = x - self.mean
        self.m2 += delta * delta2

    @property
    def std(self) -> float:
        return math.sqrt(self.m2 / self.n) if self.n >= 2 else 0.0


MIN_FLEET_SIZE_FOR_ALARM = 5  # "coordinated" is meaningless below this


class FleetBaseline:
    def __init__(self, coordinated_fraction: float, z_trigger: float, window_s: float = 60.0):
        self.coordinated_fraction = coordinated_fraction
        self.z_trigger = z_trigger
        self.window_s = window_s
        self._stats: dict[str, _RunningStat] = {name: _RunningStat() for name in FEATURE_NAMES}
        self._active_clients: set[str] = set()
        self._window_zscores: dict[str, list[tuple[str, float]]] = defaultdict(list)
        self._window_start: float | None = None

    def _zscore(self, feature: str, value: float) -> float:
        stat = self._stats[feature]
        if stat.std == 0:
            return 0.0
        return (value - stat.mean) / stat.std

    def _maybe_roll_window(self, now: float):
        if self._window_start is None:
            self._window_start = now
        elif now - self._window_start >= self.window_s:
            self._window_zscores.clear()
            self._window_start = now

    def observe(self, client_id: str, features: dict, now: float) -> tuple[float, bool]:
        """Feeds one client's just-closed feature window into the fleet
        baseline. Returns (fleet_component, coordinated_alarm) per §5.5's
        fusion rule: if a coordinated alarm is active for a feature this
        client also spiked on, fleet_component scales with the affected
        fraction; otherwise it's just this client's own capped |z|."""
        self._maybe_roll_window(now)
        self._active_clients.add(client_id)

        max_abs_z = 0.0
        for name in FEATURE_NAMES:
            value = features.get(name, 0.0)
            z = self._zscore(name, value)
            self._window_zscores[name].append((client_id, z))
            max_abs_z = max(max_abs_z, abs(z))
            self._stats[name].update(value)

        alarm, affected_fraction = self._check_coordinated(client_id)
        fleet_component = min(1.0, affected_fraction * 2) if alarm else min(1.0, max_abs_z / 6.0)
        return fleet_component, alarm

    def _check_coordinated(self, client_id: str) -> tuple[bool, float]:
        n_active = max(len(self._active_clients), 1)
        if n_active < MIN_FLEET_SIZE_FOR_ALARM:
            return False, 0.0
        for _name, entries in self._window_zscores.items():
            pos = sum(1 for cid, z in entries if z > self.z_trigger)
            neg = sum(1 for cid, z in entries if z < -self.z_trigger)
            affected = max(pos, neg)
            fraction = affected / n_active
            if affected >= 2 and fraction >= self.coordinated_fraction:
                same_direction = pos >= neg
                in_this_group = any(
                    cid == client_id and ((z > self.z_trigger) == same_direction)
                    for cid, z in entries if abs(z) > self.z_trigger
                )
                if in_this_group:
                    return True, fraction
        return False, 0.0
