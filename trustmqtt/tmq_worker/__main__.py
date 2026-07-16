"""tmq-worker entrypoint (spec §5): asyncio supervisor wiring ingest ->
features -> FSM -> drift -> fleet -> policy -> verdicts -> storage/incidents.

`ScoringContext` holds all the stateful processing logic and only needs a
redis client (not a live consumer group), so it's unit-testable without a
running Redis server; `ingest_loop` is the thin async wrapper that actually
pulls from Redis Streams and feeds events into it.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import json
import logging
import os
import time
from collections import defaultdict

import redis

from tmq_worker.config import TmqConfig, load_config
from tmq_worker.drift import DriftModelRegistry
from tmq_worker.features import (
    FEATURE_NAMES, FeatureWindow, FeatureWindowManager, compute_feature_baseline_stats,
)
from tmq_worker.fleet import FleetBaseline
from tmq_worker.fsm import BehavioralContractEngine, build_fingerprint_doc
from tmq_worker.incidents import IncidentService
from tmq_worker.ingest import Ingestor
from tmq_worker.policy import PolicyEngine, compute_trust_score
from tmq_worker.storage import (
    Client, FeatureWindowRow, Fingerprint, VerdictHistory, get_engine,
    get_or_create_client, get_sessionmaker, init_db,
)
from tmq_worker.verdicts import write_verdict

logger = logging.getLogger("tmq_worker")

FEATURE_HISTORY_CAP = 2000
DAILY_SNAPSHOT_CAP = 30
FINGERPRINT_EXPORT_INTERVAL_S = 24 * 3600


class ScoringContext:
    def __init__(self, config: TmqConfig, redis_client, fingerprint_only: bool = False):
        self.config = config
        self.redis = redis_client
        self.fingerprint_only = fingerprint_only or config.mode == "fingerprint"

        self.features = FeatureWindowManager(window_s=config.window_s)
        self.bce = BehavioralContractEngine(
            config.topic_class.numeric_suffix_regex,
            config.learning.max_events,
            config.learning.max_hours,
        )
        self.fleet = FleetBaseline(config.fleet.coordinated_fraction, config.fleet.z_trigger)
        self.drift_registry = None if self.fingerprint_only else DriftModelRegistry(redis_client)
        self.policy_engine = None if self.fingerprint_only else PolicyEngine(config.thresholds, config.hysteresis)

        self.engine = get_engine(config.database_url)
        init_db(self.engine)
        self.Session = get_sessionmaker(self.engine)
        self.incidents = IncidentService(self.Session, config) if not self.fingerprint_only else None

        self._client_username: dict[str, str] = {}
        self._feature_history: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self._daily_snapshots: dict[str, list[dict]] = defaultdict(list)

    def cohort_for(self, client_id: str) -> str:
        return self._client_username.get(client_id, "default")

    def on_event(self, evt: dict):
        if evt.get("event") == "connect" and evt.get("username"):
            self._client_username[evt["client_id"]] = evt["username"]

        self.bce.observe(evt)
        closed = self.features.process_event(evt)
        if closed:
            self._handle_closed_window(closed)

    def sweep(self, now: float):
        for closed in self.features.sweep_stale(now):
            self._handle_closed_window(closed)

    def _handle_closed_window(self, window: FeatureWindow):
        client_id = window.client_id
        fsm_score, fsm_diff = self.bce.close_window(client_id)
        window.fsm_violation_score = fsm_score

        session = self.Session()
        try:
            client = get_or_create_client(session, client_id, username=self._client_username.get(client_id))
            row = FeatureWindowRow(
                client_id=client.id,
                window_start=datetime.datetime.fromtimestamp(window.window_start, datetime.timezone.utc).replace(tzinfo=None),
                window_len_s=window.window_len_s,
                features=window.as_dict(),
                fsm_violation=fsm_score,
            )

            feature_dict = window.as_dict()
            history = self._feature_history[client_id]
            for name, value in feature_dict.items():
                values = history[name]
                values.append(value)
                if len(values) > FEATURE_HISTORY_CAP:
                    del values[: len(values) - FEATURE_HISTORY_CAP]

            if self.fingerprint_only:
                session.add(row)
                session.commit()
                return

            cohort = self.cohort_for(client_id)
            drift_model = self.drift_registry.get(cohort)
            drift_score = drift_model.score(window.to_vector())

            fleet_component, fleet_alarm = self.fleet.observe(client_id, feature_dict, now=window.window_start)

            active = None
            if self.config.adaptive_fusion:
                # A cold-start cohort's drift model is unfitted (drift ≡ 0);
                # excluding it from the fusion stops that 0 from diluting an
                # otherwise-alarming FSM/fleet signal (see policy fusion notes).
                active = {"fsm": True, "drift": drift_model.is_fitted, "fleet": True}
            trust = compute_trust_score(fsm_score, drift_score, fleet_component,
                                        self.config.weights, active=active)
            if drift_model.is_fitted:
                baseline_rate = max(0.1, float(drift_model.scaler.median[FEATURE_NAMES.index("msg_rate")]))
            else:
                baseline_rate = max(0.1, window.msg_rate)
            is_learning = self.bce.is_learning(client_id)

            reason = f"fsm={fsm_score:.2f} drift={drift_score:.2f} fleet={fleet_component:.2f}"
            verdict = self.policy_engine.evaluate(client_id, trust, baseline_rate,
                                                    is_learning=is_learning, reason=reason)

            row.drift = drift_score
            row.fleet = fleet_component
            row.trust = trust
            session.add(row)
            session.add(VerdictHistory(client_id=client.id, level=int(verdict.level),
                                        score=verdict.score, reason=verdict.reason))
            session.commit()

            write_verdict(self.redis, client_id, verdict)

            if self.incidents:
                self.incidents.maybe_open_or_update(session, client, verdict, trust, fsm_diff, fleet_alarm)
        finally:
            session.close()

    def export_fingerprints(self, fingerprints_dir: str):
        """Per-client fingerprint export (spec §5.6): Postgres row +
        `fingerprints/<client_id>.json`. Meant to be called periodically
        (daily in production) so `stability` reflects successive daily FSM
        snapshots, not just a single point-in-time transition matrix."""
        os.makedirs(fingerprints_dir, exist_ok=True)
        session = self.Session()
        try:
            for client_id, fsm in list(self.bce._fsms.items()):
                history = self._daily_snapshots[client_id]
                history.append(fsm.snapshot())
                if len(history) > DAILY_SNAPSHOT_CAP:
                    del history[: len(history) - DAILY_SNAPSHOT_CAP]

                baseline = compute_feature_baseline_stats(self._feature_history.get(client_id, {}))
                doc = build_fingerprint_doc(fsm, baseline, history)

                client = get_or_create_client(session, client_id, username=self._client_username.get(client_id))
                session.add(Fingerprint(client_id=client.id, doc=doc, stability=doc["stability"]))
                session.commit()

                path = os.path.join(fingerprints_dir, f"{client_id}.json")
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(doc, f, indent=2)
        finally:
            session.close()


async def ingest_loop(ctx: ScoringContext, ingestor: Ingestor):
    while True:
        batch = await asyncio.to_thread(ingestor.read_batch, 512, 1000)
        for _entry_id, evt in batch:
            ctx.on_event(evt.model_dump())


async def sweep_loop(ctx: ScoringContext, interval_s: float = 5.0):
    while True:
        await asyncio.sleep(interval_s)
        ctx.sweep(time.time())


async def fingerprint_export_loop(ctx: ScoringContext, fingerprints_dir: str,
                                   interval_s: float = FINGERPRINT_EXPORT_INTERVAL_S):
    while True:
        await asyncio.sleep(interval_s)
        await asyncio.to_thread(ctx.export_fingerprints, fingerprints_dir)


async def run_async(config: TmqConfig, fingerprint_only: bool):
    redis_client = redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True)
    ctx = ScoringContext(config, redis_client, fingerprint_only=fingerprint_only)
    ingestor = Ingestor(redis_client, consumer_name=f"worker-{os.getpid()}")
    logger.info("tmq_worker starting (mode=%s, fingerprint_only=%s)", config.mode, ctx.fingerprint_only)

    tasks = [ingest_loop(ctx, ingestor), sweep_loop(ctx)]
    if ctx.fingerprint_only:
        fingerprints_dir = os.environ.get("TMQ_FINGERPRINTS_DIR", "fingerprints")
        tasks.append(fingerprint_export_loop(ctx, fingerprints_dir))
    await asyncio.gather(*tasks)


def main():
    parser = argparse.ArgumentParser(description="TrustMQTT scoring worker")
    parser.add_argument("--fingerprint-only", action="store_true",
                         help="Run ingest+features+FSM only; skip drift/policy/verdicts (spec §5.6)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    config = load_config()
    asyncio.run(run_async(config, args.fingerprint_only))


if __name__ == "__main__":
    main()
