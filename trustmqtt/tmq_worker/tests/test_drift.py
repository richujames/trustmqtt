import json

import numpy as np
import pytest

from tmq_worker.drift import DriftModel, DriftModelRegistry, fit_robust_scaler
from tmq_worker.features import FEATURE_NAMES


def _make_normal_training_set(n=200, seed=0):
    rng = np.random.default_rng(seed)
    n_features = len(FEATURE_NAMES)
    # Tight cluster around a plausible "benign IoT sensor" feature vector.
    base = np.zeros(n_features)
    base[FEATURE_NAMES.index("msg_rate")] = 1.0
    base[FEATURE_NAMES.index("qos1_ratio")] = 1.0
    base[FEATURE_NAMES.index("unique_topics")] = 1.0
    return base + rng.normal(scale=0.05, size=(n, n_features))


def test_unfitted_model_scores_zero_cold_start():
    model = DriftModel()
    assert model.is_fitted is False
    assert model.score([0.0] * len(FEATURE_NAMES)) == 0.0


def test_fit_requires_minimum_samples():
    model = DriftModel()
    with pytest.raises(ValueError):
        model.fit(np.zeros((3, len(FEATURE_NAMES))))


def test_anomalous_point_scores_higher_than_normal_point():
    X = _make_normal_training_set()
    model = DriftModel()
    model.fit(X)
    assert model.is_fitted is True

    normal_point = X[0].tolist()
    anomalous_point = list(X[0])
    # Blow out msg_rate and byte_rate way outside the training distribution.
    anomalous_point[FEATURE_NAMES.index("msg_rate")] = 500.0
    anomalous_point[FEATURE_NAMES.index("byte_rate")] = 5_000_000.0
    anomalous_point[FEATURE_NAMES.index("new_topic_ratio")] = 1.0

    normal_score = model.score(normal_point)
    anomalous_score = model.score(anomalous_point)

    assert 0.0 <= normal_score <= 1.0
    assert 0.0 <= anomalous_score <= 1.0
    assert anomalous_score > normal_score


def test_save_and_load_roundtrip(tmp_path):
    X = _make_normal_training_set()
    model = DriftModel()
    model.fit(X)
    path = tmp_path / "cohort.pkl"
    model.save(str(path))

    loaded = DriftModel.load(str(path))
    assert loaded.is_fitted is True
    assert loaded.n_samples == model.n_samples
    assert loaded.score(X[0].tolist()) == pytest.approx(model.score(X[0].tolist()))


def test_robust_scaler_centers_on_median_and_scales_by_iqr():
    X = np.array([[1.0], [2.0], [3.0], [4.0], [100.0]])
    scaler = fit_robust_scaler(X)
    transformed = scaler.transform(X)
    # Median of [1,2,3,4,100] is 3; a robust scaler should be far less
    # distorted by the 100 outlier than a mean/std scaler would be.
    assert abs(transformed[2, 0]) < 1e-9  # the median itself maps to ~0


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def hget(self, key, field):
        return self.store.get((key, field))

    def hset(self, key, field, value):
        self.store[(key, field)] = value


def test_registry_cold_start_then_hot_reload(tmp_path):
    r = _FakeRedis()
    registry = DriftModelRegistry(r)

    # No metadata published yet -> cold start, score is always 0.
    model = registry.get("plant-a")
    assert model.score([0.0] * len(FEATURE_NAMES)) == 0.0

    X = _make_normal_training_set()
    trained = DriftModel()
    trained.fit(X)
    path = tmp_path / "plant-a.pkl"
    trained.save(str(path))
    r.hset("tmq:models:meta", "plant-a", json.dumps({"trained_at": 123.0, "n_samples": len(X), "path": str(path)}))

    model = registry.get("plant-a")
    assert model.is_fitted is True

    # Same trained_at -> no reload needed, same object reused.
    same_model = registry.get("plant-a")
    assert same_model is model
