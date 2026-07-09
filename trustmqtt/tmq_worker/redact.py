"""Mandatory redaction layer (spec §7.1, I8) — the only sanctioned bridge
between incident data and llm.py. incidents.py must call this before
anything reaches `generate_report`; nothing here ever imports from
ingest.py or touches the raw `tmq:events` stream (spec §12 rule 6).
"""
from __future__ import annotations

import hashlib
import hmac
import re
from typing import Optional


def mask_ip(ip: str) -> str:
    """10.0.3.17 -> 10.0.3.x (mask to /24). Leaves anything that isn't a
    plain dotted-quad (IPv6, already-masked, empty) alone rather than
    guessing at a wrong transformation."""
    if not ip:
        return ip
    parts = ip.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        return ".".join(parts[:3] + ["x"])
    return ip


def pseudonymize(value: str, secret: str) -> str:
    """HMAC-SHA256(value) truncated to a short id, e.g. "client-7f3a"."""
    if not value:
        return value
    digest = hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"client-{digest[:4]}"


def _pattern_to_regex(pattern: str) -> re.Pattern:
    segments = pattern.split("/")
    parts = []
    for seg in segments:
        if seg == "+":
            parts.append(r"[^/]+")
        elif seg == "#":
            parts.append(r".*")
        else:
            parts.append(re.escape(seg))
    return re.compile("^" + "/".join(parts) + "$")


def redact_topic(topic: str, secret_topic_patterns: list[str]) -> str:
    """Replaces the wildcard-matched segments of `topic` with a redaction
    marker if it matches one of the configured secret-topic filters (spec
    §8 `redaction.secret_topic_patterns`, e.g. "+/credentials/#")."""
    if not topic:
        return topic
    for pattern in secret_topic_patterns:
        if _pattern_to_regex(pattern).match(topic):
            pattern_segments = pattern.split("/")
            topic_segments = topic.split("/")
            out = []
            for i, seg in enumerate(topic_segments):
                if i < len(pattern_segments) and pattern_segments[i] in ("+", "#"):
                    out.append("⟦redacted⟧")
                else:
                    out.append(seg)
            return "/".join(out)
    return topic


def _redact_transition_symbol(symbol: Optional[str], secret_topic_patterns: list[str]) -> Optional[str]:
    """FSM alphabet symbols look like "PUB(topic/class,qos)" or
    "SUB(topic/class)" — pull the embedded topic class out, redact it, and
    rebuild the symbol string."""
    if not symbol:
        return symbol
    m = re.match(r"^(PUB|SUB|UNSUB)\((.*)\)$", symbol)
    if not m:
        return symbol
    kind, inner = m.group(1), m.group(2)
    if kind == "PUB" and "," in inner:
        topic, qos = inner.rsplit(",", 1)
        return f"PUB({redact_topic(topic, secret_topic_patterns)},{qos})"
    return f"{kind}({redact_topic(inner, secret_topic_patterns)})"


def redact_incident_summary(summary: dict, secret: str, secret_topic_patterns: list[str]) -> dict:
    """Redacts an incident summary struct (spec §7.1: "only the incident
    summary struct — scores, level history, FSM-diff transition names,
    window stats"). Never accepts or forwards raw per-message events."""
    out = dict(summary)

    if out.get("client_id"):
        out["client_id"] = pseudonymize(out["client_id"], secret)
    if out.get("username"):
        out["username"] = pseudonymize(out["username"], secret)
    if out.get("ip"):
        out["ip"] = mask_ip(out["ip"])

    out.pop("payload_sha256", None)

    if out.get("fsm_diff"):
        out["fsm_diff"] = [
            {
                **d,
                "from": _redact_transition_symbol(d.get("from"), secret_topic_patterns),
                "to": _redact_transition_symbol(d.get("to"), secret_topic_patterns),
            }
            for d in out["fsm_diff"]
        ]

    return out
