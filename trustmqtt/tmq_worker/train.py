"""Offline/periodic drift-model trainer CLI (spec §5.3, §10 Phase 4).

Pulls feature windows from Postgres, fits one DriftModel per cohort
(default: username; --per-client for one model per client_id), writes the
model to the shared models/ volume, and publishes its metadata to both
Postgres (model_versions row, spec §7.2) and Redis (`tmq:models:meta`
hash, spec §4.4) so running workers hot-reload it without a restart.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict

import numpy as np
import redis

from tmq_worker.config import load_config
from tmq_worker.drift import DriftModel
from tmq_worker.storage import (
    Client, FeatureWindowRow, ModelVersion, get_engine, get_sessionmaker,
)


def _cohort_for_client(client: Client, per_client: bool) -> str:
    if per_client:
        return client.client_id
    return client.cohort or client.username or "default"


def collect_training_data(session, per_client: bool) -> dict[str, list[list[float]]]:
    data: dict[str, list[list[float]]] = defaultdict(list)
    client_cache: dict[int, Client] = {}
    feature_names = DriftModel().feature_names

    for row in session.query(FeatureWindowRow).all():
        client = client_cache.get(row.client_id)
        if client is None:
            client = session.get(Client, row.client_id)
            client_cache[row.client_id] = client
        if client is None:
            continue
        cohort = _cohort_for_client(client, per_client)
        vec = [row.features.get(name, 0.0) for name in feature_names]
        data[cohort].append(vec)

    return data


def train_all(config, models_dir: str, per_client: bool = False) -> list[str]:
    engine = get_engine(config.database_url)
    session = get_sessionmaker(engine)()
    r = redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True)

    data = collect_training_data(session, per_client)
    os.makedirs(models_dir, exist_ok=True)
    trained_cohorts = []

    for cohort, vectors in data.items():
        if len(vectors) < 10:
            continue
        model = DriftModel()
        model.fit(np.array(vectors, dtype=float))

        path = os.path.join(models_dir, f"{cohort}.pkl")
        model.save(path)

        trained_at = time.time()
        session.add(ModelVersion(cohort=cohort, n_samples=len(vectors), metrics={}, path=path))
        session.commit()

        r.hset("tmq:models:meta", cohort, json.dumps({
            "trained_at": trained_at,
            "n_samples": len(vectors),
            "path": path,
        }))
        trained_cohorts.append(cohort)

    return trained_cohorts


def main():
    parser = argparse.ArgumentParser(description="Train TrustMQTT drift models")
    parser.add_argument("--per-client", action="store_true",
                         help="Train one model per client_id instead of per cohort")
    parser.add_argument("--models-dir", default=os.environ.get("TMQ_MODELS_DIR", "models"))
    args = parser.parse_args()

    config = load_config()
    trained = train_all(config, args.models_dir, per_client=args.per_client)
    if trained:
        print(f"Trained {len(trained)} cohort model(s): {', '.join(trained)}")
    else:
        print("No cohort had enough feature windows (>=10) to train yet.")


if __name__ == "__main__":
    main()
