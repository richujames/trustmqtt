"""Tumbling-window feature extraction (spec docs/SPEC.md §4.3, §5.1).

Resolved-semantic-event analysis only: every feature below is derived from
topic, QoS, retain, payload length, session/keepalive state and MQTT5
properties observed by the broker plugin. None of it can see (and none of
the downstream FSM/drift/policy stages can therefore ever depend on) the
DUP flag, packet identifiers, PUBACK/PUBREC/PUBREL/PUBCOMP reason codes,
PINGREQ/PINGRESP timing, or topic-alias usage — those simply are not
observable through the Mosquitto plugin event hooks (docs/SPEC.md §1.2,
§12 rule 4).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional

FEATURE_NAMES = [
    "msg_rate", "byte_rate", "mean_iat", "std_iat",
    "unique_topics", "new_topic_ratio",
    "qos0_ratio", "qos1_ratio", "qos2_ratio", "retain_ratio",
    "sub_events", "unsub_events", "sub_count_delta",
    "topic_entropy", "mean_payload_len", "std_payload_len",
    "ka_conformance", "silent_alive_ratio",
    "connect_events", "disconnect_events", "fsm_violation_score",
]


@dataclass
class FeatureWindow:
    client_id: str
    window_start: float
    window_len_s: float
    msg_rate: float = 0.0
    byte_rate: float = 0.0
    mean_iat: float = 0.0
    std_iat: float = 0.0
    unique_topics: int = 0
    new_topic_ratio: float = 0.0
    qos0_ratio: float = 0.0
    qos1_ratio: float = 0.0
    qos2_ratio: float = 0.0
    retain_ratio: float = 0.0
    sub_events: int = 0
    unsub_events: int = 0
    sub_count_delta: int = 0
    topic_entropy: float = 0.0
    mean_payload_len: float = 0.0
    std_payload_len: float = 0.0
    ka_conformance: float = 0.0
    silent_alive_ratio: float = 0.0
    connect_events: int = 0
    disconnect_events: int = 0
    fsm_violation_score: float = 0.0  # joined in later by the FSM stage

    def to_vector(self) -> list[float]:
        return [getattr(self, name) for name in FEATURE_NAMES]

    def as_dict(self) -> dict:
        return {name: getattr(self, name) for name in FEATURE_NAMES}


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: list[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / len(xs))


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


def compute_feature_baseline_stats(history: dict) -> dict:
    """Per-feature {med, iqr} summary used as the fingerprint document's
    `feature_baseline` field (spec §5.6)."""
    out = {}
    for name, values in history.items():
        if not values:
            continue
        s = sorted(values)
        med = _percentile(s, 0.5)
        iqr = _percentile(s, 0.75) - _percentile(s, 0.25)
        out[name] = {"med": round(med, 4), "iqr": round(iqr, 4)}
    return out


def _entropy(counts: dict) -> float:
    total = sum(counts.values())
    if total == 0:
        return 0.0
    h = 0.0
    for c in counts.values():
        if c == 0:
            continue
        p = c / total
        h -= p * math.log2(p)
    return h


@dataclass
class _ClientAccumulator:
    client_id: str
    window_start: float
    keepalive: Optional[int] = None
    publish_ts: list[float] = field(default_factory=list)
    activity_ts: list[float] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    qos_counts: dict = field(default_factory=lambda: {0: 0, 1: 0, 2: 0})
    retain_count: int = 0
    payload_lens: list[float] = field(default_factory=list)
    sub_events: int = 0
    unsub_events: int = 0
    sub_count_samples: list[int] = field(default_factory=list)
    ka_gap_seconds_sum: float = 0.0
    connect_events: int = 0
    disconnect_events: int = 0

    def close(self, window_len_s: float, known_topics: set) -> FeatureWindow:
        publish_count = len(self.publish_ts)
        unique_topics_set = set(self.topics)
        new_topics = unique_topics_set - known_topics
        topic_counts: dict = {}
        for t in self.topics:
            topic_counts[t] = topic_counts.get(t, 0) + 1

        iats = [b - a for a, b in zip(self.publish_ts, self.publish_ts[1:])]
        activity_iats = [b - a for a, b in zip(self.activity_ts, self.activity_ts[1:])]

        ka_conformance = 0.0
        if self.keepalive and self.keepalive > 0 and len(activity_iats) >= 2:
            ka_conformance = _percentile(activity_iats, 0.95) / self.keepalive

        sub_count_delta = 0
        if len(self.sub_count_samples) >= 2:
            sub_count_delta = self.sub_count_samples[-1] - self.sub_count_samples[0]

        return FeatureWindow(
            client_id=self.client_id,
            window_start=self.window_start,
            window_len_s=window_len_s,
            msg_rate=publish_count / window_len_s,
            byte_rate=sum(self.payload_lens) / window_len_s,
            mean_iat=_mean(iats),
            std_iat=_std(iats),
            unique_topics=len(unique_topics_set),
            new_topic_ratio=(len(new_topics) / len(unique_topics_set)) if unique_topics_set else 0.0,
            qos0_ratio=(self.qos_counts[0] / publish_count) if publish_count else 0.0,
            qos1_ratio=(self.qos_counts[1] / publish_count) if publish_count else 0.0,
            qos2_ratio=(self.qos_counts[2] / publish_count) if publish_count else 0.0,
            retain_ratio=(self.retain_count / publish_count) if publish_count else 0.0,
            sub_events=self.sub_events,
            unsub_events=self.unsub_events,
            sub_count_delta=sub_count_delta,
            topic_entropy=_entropy(topic_counts),
            mean_payload_len=_mean(self.payload_lens),
            std_payload_len=_std(self.payload_lens),
            ka_conformance=ka_conformance,
            silent_alive_ratio=min(1.0, self.ka_gap_seconds_sum / window_len_s) if window_len_s > 0 else 0.0,
            connect_events=self.connect_events,
            disconnect_events=self.disconnect_events,
        )


class FeatureWindowManager:
    """Maintains one tumbling window per client_id (spec §4.3).

    A client's window rolls over to the next `window_s`-sized bucket either
    when an event for that client arrives with a timestamp past the current
    window's end, or when `sweep_stale()` is called periodically by the
    main loop and finds windows that have aged out with no new events.
    `known_topics` persists per-client across windows (not reset) so
    new_topic_ratio can tell a genuinely new topic from a repeat.
    """

    def __init__(self, window_s: float):
        self.window_s = window_s
        self._acc: dict[str, _ClientAccumulator] = {}
        self._known_topics: dict[str, set] = {}

    def _get_or_open(self, client_id: str, ts: float) -> _ClientAccumulator:
        acc = self._acc.get(client_id)
        if acc is None:
            acc = _ClientAccumulator(client_id=client_id, window_start=ts)
            self._acc[client_id] = acc
        return acc

    def _roll_if_needed(self, client_id: str, ts: float) -> Optional[FeatureWindow]:
        acc = self._acc.get(client_id)
        if acc is None:
            return None
        if ts < acc.window_start + self.window_s:
            return None
        closed = self._close(client_id)
        self._acc[client_id] = _ClientAccumulator(client_id=client_id, window_start=acc.window_start + self.window_s)
        return closed

    def _close(self, client_id: str) -> FeatureWindow:
        acc = self._acc[client_id]
        known = self._known_topics.setdefault(client_id, set())
        closed = acc.close(self.window_s, known)
        known.update(acc.topics)
        return closed

    def process_event(self, evt: dict) -> Optional[FeatureWindow]:
        """Feeds one validated event dict (ingest.TmqEvent.model_dump()) into
        the appropriate client's window. Returns a completed FeatureWindow if
        this event rolled a window over, else None."""
        client_id = evt.get("client_id")
        if not client_id:
            return None
        ts = evt["ts"]
        closed = self._roll_if_needed(client_id, ts)
        acc = self._get_or_open(client_id, ts)

        event_type = evt["event"]
        if event_type == "connect":
            acc.connect_events += 1
            if evt.get("keepalive"):
                acc.keepalive = evt["keepalive"]
        elif event_type in ("disconnect", "client_offline"):
            acc.disconnect_events += 1
        elif event_type == "publish":
            acc.publish_ts.append(ts)
            acc.activity_ts.append(ts)
            topic = evt.get("topic")
            if topic:
                acc.topics.append(topic)
            qos = evt.get("qos", 0)
            if qos in acc.qos_counts:
                acc.qos_counts[qos] += 1
            if evt.get("retain"):
                acc.retain_count += 1
            acc.payload_lens.append(float(evt.get("payload_len", 0)))
        elif event_type in ("subscribe", "unsubscribe"):
            acc.activity_ts.append(ts)
            if event_type == "subscribe":
                acc.sub_events += 1
            else:
                acc.unsub_events += 1
            if "sub_count" in evt:
                acc.sub_count_samples.append(evt["sub_count"])
        elif event_type == "ka_gap":
            acc.ka_gap_seconds_sum += float(evt.get("gap_s", 0.0))
            if evt.get("keepalive"):
                acc.keepalive = evt["keepalive"]

        return closed

    def sweep_stale(self, now: float, idle_grace_s: float = 5.0) -> list[FeatureWindow]:
        """Closes windows for clients that have gone quiet past their window
        boundary + grace period, so a client that stops sending events
        entirely doesn't leave its last partial window stuck open forever."""
        closed_windows = []
        for client_id, acc in list(self._acc.items()):
            if now >= acc.window_start + self.window_s + idle_grace_s:
                closed_windows.append(self._close(client_id))
                del self._acc[client_id]
        return closed_windows
