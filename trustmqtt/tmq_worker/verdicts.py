"""Redis verdict writer (spec §4.2): writes the packed string (hot path,
read by the plugin's TICK refresher) and the human-readable hash
(Grafana/ops) in a single MULTI transaction so they can't drift apart."""
from __future__ import annotations

import time

from tmq_worker.policy import Verdict

VERDICT_TTL_S = 120


def write_verdict(redis_client, client_id: str, verdict: Verdict, now: float | None = None) -> None:
    now = now if now is not None else time.time()
    expires_at = now + VERDICT_TTL_S
    packed = f"{int(verdict.level)}|{verdict.score:.4f}|{int(expires_at)}|{verdict.rate:.4f}"

    pipe = redis_client.pipeline(transaction=True)
    pipe.set(f"tmq:verdictp:{client_id}", packed, ex=VERDICT_TTL_S)
    pipe.hset(f"tmq:verdict:{client_id}", mapping={
        "level": int(verdict.level),
        "score": verdict.score,
        "expires_at": expires_at,
        "rate": verdict.rate,
        "reason": verdict.reason,
        "updated_at": now,
    })
    pipe.expire(f"tmq:verdict:{client_id}", VERDICT_TTL_S)
    pipe.execute()
