"""Behavioral Contract Engine — per-client FSM over a resolved-semantic-event
alphabet (spec §5.2). Learns a first-order Markov transition matrix per
client during a learning period, then keeps adapting slowly (exponential
decay) so legitimate behavior drift doesn't get permanently flagged.
"""
from __future__ import annotations

import math
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

P_FLOOR = 1e-4
DECAY_ALPHA = 0.01
DEFAULT_NOVEL_P_THRESHOLD = 0.01


def _percentile(xs: list[float], pct: float) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    if len(s) == 1:
        return s[0]
    idx = pct * (len(s) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return s[lo]
    frac = idx - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def topic_class(topic: str, numeric_suffix_regex: str) -> str:
    """Normalizes a topic into a class by replacing segments that look like
    a numeric-suffixed identifier (e.g. "line2") with '+', while leaving
    genuinely descriptive segments (e.g. "plant-a", "temp") alone. Example
    (spec's own): "plant-a/line2/temp" -> "plant-a/+/temp"."""
    pattern = re.compile(numeric_suffix_regex)
    return "/".join("+" if pattern.match(seg) else seg for seg in topic.split("/"))


def symbol_for_event(evt: dict, numeric_suffix_regex: str) -> Optional[str]:
    """Maps a validated event dict onto the FSM alphabet. Events with no
    FSM meaning (auth_observe, plugin_stats) return None."""
    et = evt["event"]
    if et == "connect":
        return "CONNECT"
    if et == "disconnect":
        return "DISCONNECT"
    if et == "client_offline":
        return "OFFLINE"
    if et == "ka_gap":
        return "KA_GAP"
    if et == "subscribe":
        return f"SUB({topic_class(evt.get('topic', ''), numeric_suffix_regex)})"
    if et == "unsubscribe":
        return f"UNSUB({topic_class(evt.get('topic', ''), numeric_suffix_regex)})"
    if et == "publish":
        tc = topic_class(evt.get("topic", ""), numeric_suffix_regex)
        return f"PUB({tc},{evt.get('qos', 0)})"
    return None


@dataclass
class ClientFSM:
    client_id: str
    numeric_suffix_regex: str
    learning_max_events: int
    learning_max_hours: float
    first_event_ts: Optional[float] = None
    last_event_ts: Optional[float] = None
    event_count: int = 0
    learning_complete: bool = False
    last_symbol: Optional[str] = None
    counts: dict = field(default_factory=lambda: defaultdict(dict))

    def _vocab_size(self) -> int:
        symbols = set(self.counts.keys())
        for row in self.counts.values():
            symbols.update(row.keys())
        return max(len(symbols), 1)

    def _prob(self, prev: str, cur: str) -> float:
        vocab = self._vocab_size()
        row = self.counts.get(prev)
        if not row:
            return 1.0 / (vocab + 1)  # Laplace smoothing over an unseen prev-state row
        total = sum(row.values())
        return (row.get(cur, 0.0) + 1.0) / (total + vocab)

    def _maybe_finish_learning(self, ts: float):
        if self.learning_complete:
            return
        if self.event_count >= self.learning_max_events:
            self.learning_complete = True
            return
        if self.first_event_ts is not None and (ts - self.first_event_ts) >= self.learning_max_hours * 3600:
            self.learning_complete = True

    def observe(self, evt: dict) -> Optional[float]:
        """Feeds one event into the FSM. Returns the transition's violation
        score (0..1), or None if the event has no FSM symbol."""
        sym = symbol_for_event(evt, self.numeric_suffix_regex)
        if sym is None:
            return None

        ts = evt["ts"]
        if self.first_event_ts is None:
            self.first_event_ts = ts
        self.last_event_ts = ts
        self.event_count += 1

        viol = None
        if self.last_symbol is not None:
            p = self._prob(self.last_symbol, sym)
            viol = min(1.0, -math.log(p) / -math.log(P_FLOOR))

            row = self.counts[self.last_symbol]
            if self.learning_complete:
                for k in list(row.keys()):
                    row[k] *= (1 - DECAY_ALPHA)
            row[sym] = row.get(sym, 0.0) + 1.0

        self._maybe_finish_learning(ts)
        self.last_symbol = sym
        return viol

    def top_transitions(self, k: int = 5) -> list:
        pairs = []
        for prev, row in self.counts.items():
            for cur in row:
                pairs.append((prev, cur, self._prob(prev, cur)))
        pairs.sort(key=lambda x: x[2], reverse=True)
        return pairs[:k]

    def novel_transitions(self, observed_pairs: set, p_threshold: float = DEFAULT_NOVEL_P_THRESHOLD) -> list:
        """FSM-diff (I7): observed (prev, cur) pairs whose learned
        probability is below `p_threshold`."""
        out = []
        for prev, cur in observed_pairs:
            if prev is None:
                continue
            p = self._prob(prev, cur)
            if p < p_threshold:
                out.append({"from": prev, "to": cur, "learned_p": p})
        out.sort(key=lambda d: d["learned_p"])
        return out

    def to_json_doc(self) -> dict:
        states = set(self.counts.keys())
        for row in self.counts.values():
            states.update(row.keys())
        return {
            "client_id": self.client_id,
            "learning_complete": self.learning_complete,
            "event_count": self.event_count,
            "states": sorted(states),
            "top_transitions": [[p, c, round(pr, 4)] for p, c, pr in self.top_transitions()],
        }

    def snapshot(self) -> dict:
        """A normalized probability distribution over every (prev, cur)
        transition observed so far, used as one "daily" data point for the
        fingerprint stability metric (spec §5.6)."""
        dist: dict = {}
        total = 0.0
        for prev, row in self.counts.items():
            for cur, c in row.items():
                dist[f"{prev}->{cur}"] = c
                total += c
        if total == 0:
            return {}
        return {k: v / total for k, v in dist.items()}


def js_divergence(p: dict, q: dict) -> float:
    """Jensen-Shannon divergence (base-2, in [0, 1]) between two transition
    snapshots produced by `ClientFSM.snapshot()`."""
    keys = set(p.keys()) | set(q.keys())
    if not keys:
        return 0.0

    m = {k: 0.5 * (p.get(k, 0.0) + q.get(k, 0.0)) for k in keys}

    def _kl(a: dict) -> float:
        s = 0.0
        for k in keys:
            ak = a.get(k, 0.0)
            mk = m.get(k, 0.0)
            if ak > 0 and mk > 0:
                s += ak * math.log2(ak / mk)
        return s

    return 0.5 * _kl(p) + 0.5 * _kl(q)


def compute_stability(daily_snapshots: list[dict]) -> float:
    """stability = 1 - mean JS-divergence between successive daily
    transition-matrix snapshots (spec §5.6). Defaults to 1.0 (nothing to
    contradict stability yet) with fewer than 2 snapshots."""
    if len(daily_snapshots) < 2:
        return 1.0
    divs = [js_divergence(a, b) for a, b in zip(daily_snapshots, daily_snapshots[1:])]
    return 1.0 - (sum(divs) / len(divs))


def build_fingerprint_doc(fsm: ClientFSM, feature_baseline: dict, daily_snapshots: list[dict]) -> dict:
    """Assembles the fingerprint document described in spec §5.6."""
    doc = fsm.to_json_doc()
    hours = 0.0
    if fsm.first_event_ts is not None and fsm.last_event_ts is not None:
        hours = (fsm.last_event_ts - fsm.first_event_ts) / 3600.0
    return {
        "client_id": fsm.client_id,
        "learned_over": {"events": fsm.event_count, "hours": round(hours, 2)},
        "fsm": {"states": doc["states"], "top_transitions": doc["top_transitions"]},
        "feature_baseline": feature_baseline,
        "stability": compute_stability(daily_snapshots),
    }


class BehavioralContractEngine:
    """Owns one ClientFSM per client_id, plus the per-window violation/pair
    buffers needed to produce `fsm_violation_score` (joined into
    FeatureWindow, spec §4.3) and the FSM-diff (spec §7.2/I7) when a
    window closes."""

    def __init__(self, numeric_suffix_regex: str, learning_max_events: int, learning_max_hours: float):
        self.numeric_suffix_regex = numeric_suffix_regex
        self.learning_max_events = learning_max_events
        self.learning_max_hours = learning_max_hours
        self._fsms: dict[str, ClientFSM] = {}
        self._window_violations: dict[str, list] = defaultdict(list)
        self._window_pairs: dict[str, set] = defaultdict(set)

    def _get(self, client_id: str) -> ClientFSM:
        fsm = self._fsms.get(client_id)
        if fsm is None:
            fsm = ClientFSM(client_id, self.numeric_suffix_regex,
                             self.learning_max_events, self.learning_max_hours)
            self._fsms[client_id] = fsm
        return fsm

    def observe(self, evt: dict):
        client_id = evt.get("client_id")
        if not client_id:
            return
        fsm = self._get(client_id)
        prev_symbol = fsm.last_symbol
        viol = fsm.observe(evt)
        if viol is not None:
            self._window_violations[client_id].append(viol)
            self._window_pairs[client_id].add((prev_symbol, fsm.last_symbol))

    def is_learning(self, client_id: str) -> bool:
        fsm = self._fsms.get(client_id)
        return fsm is None or not fsm.learning_complete

    def close_window(self, client_id: str) -> tuple[float, list]:
        """Returns (fsm_violation_score, fsm_diff) for the just-closed
        window and resets that client's per-window buffers."""
        viols = self._window_violations.pop(client_id, [])
        pairs = self._window_pairs.pop(client_id, set())
        score = _percentile(viols, 0.95) if viols else 0.0
        fsm = self._fsms.get(client_id)
        diff = fsm.novel_transitions(pairs) if fsm else []
        return score, diff

    def serialize(self, client_id: str) -> Optional[dict]:
        fsm = self._fsms.get(client_id)
        return fsm.to_json_doc() if fsm else None
