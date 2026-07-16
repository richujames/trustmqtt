"""Redis Streams consumer-group reader (spec §5.1).

`parse_entry` is kept free of any Redis I/O so it can be unit-tested
directly against hand-built stream field dicts, without a running Redis.
"""
from __future__ import annotations

import json
from typing import Literal, Optional

import redis
from pydantic import BaseModel, ValidationError

STREAM = "tmq:events"
DEAD_STREAM = "tmq:events:dead"
GROUP = "tmqw"

EventType = Literal[
    "connect", "disconnect", "client_offline", "publish", "subscribe",
    "unsubscribe", "auth_observe", "ka_gap", "plugin_stats",
]


class Props(BaseModel):
    content_type: Optional[str] = None
    message_expiry: Optional[int] = None
    user_prop_count: Optional[int] = None


class TmqEvent(BaseModel):
    v: int
    ts: float
    event: EventType
    client_id: str
    username: Optional[str] = None
    ip: Optional[str] = None
    protocol: Optional[str] = None
    clean_session: Optional[bool] = None
    keepalive: Optional[int] = None
    reason: Optional[int] = None
    topic: Optional[str] = None
    qos: Optional[int] = None
    retain: Optional[bool] = None
    payload_len: Optional[int] = None
    payload_sha256: Optional[str] = None
    props: Optional[Props] = None
    sub_count: Optional[int] = None
    gap_s: Optional[float] = None
    dropped_events: Optional[int] = None
    ring_size: Optional[int] = None


class MalformedEvent(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def parse_entry(fields: dict) -> TmqEvent:
    """Parses one Redis stream entry's field dict (as produced by the C
    plugin's `XADD tmq:events ... v <json>`, i.e. a single field literally
    named "v" holding the whole event JSON blob) into a validated TmqEvent.
    Raises MalformedEvent on anything that doesn't parse/validate."""
    raw = fields.get("v")
    if raw is None:
        raise MalformedEvent("missing 'v' field")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise MalformedEvent(f"invalid JSON: {e}") from e
    try:
        return TmqEvent.model_validate(data)
    except ValidationError as e:
        raise MalformedEvent(f"schema validation failed: {e}") from e


class Ingestor:
    """Thin wrapper around XREADGROUP that validates and dead-letters as it
    goes (spec §5.1). ACKs immediately after successful parse+validate —
    that's "routing" in spec terms; a downstream processing failure is a
    worker bug to fix and redeploy, not something we want to redeliver
    forever from Redis's pending-entries list."""

    def __init__(self, redis_client: "redis.Redis", consumer_name: str):
        self.r = redis_client
        self.consumer_name = consumer_name
        self.dead_count = 0
        self._ensure_group()

    def _ensure_group(self):
        try:
            self.r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def read_batch(self, count: int = 512, block_ms: int = 1000) -> list[tuple[str, TmqEvent]]:
        resp = self.r.xreadgroup(GROUP, self.consumer_name, {STREAM: ">"}, count=count, block=block_ms)
        results: list[tuple[str, TmqEvent]] = []
        if not resp:
            return results
        ack_ids: list[str] = []
        for _stream_name, entries in resp:
            for entry_id, fields in entries:
                try:
                    evt = parse_entry(fields)
                    results.append((entry_id, evt))
                except MalformedEvent as e:
                    self._deadletter(entry_id, fields, e.reason)
                ack_ids.append(entry_id)
        # One XACK for the whole batch instead of one round-trip per entry
        # (a 512-event batch was previously up to 512 separate ACK calls).
        if ack_ids:
            self.r.xack(STREAM, GROUP, *ack_ids)
        return results

    def _deadletter(self, entry_id: str, fields: dict, error: str):
        self.dead_count += 1
        payload = {"original_id": entry_id, "error": error}
        payload.update(fields)
        self.r.xadd(DEAD_STREAM, payload, maxlen=100000, approximate=True)
