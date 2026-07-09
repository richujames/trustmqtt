"""Statistical Drift Scorer — IsolationForest + OneClassSVM ensemble over
the §4.3 feature vector, min-max fused into a single 0..1 score (spec §5.3).
"""
from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from typing import Optional

import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM

from tmq_worker.features import FEATURE_NAMES

IF_PARAMS = dict(n_estimators=200, contamination=0.02, random_state=42)
OCSVM_PARAMS = dict(kernel="rbf", nu=0.05, gamma="scale")
MIN_TRAINING_SAMPLES = 10


@dataclass
class RobustScalerParams:
    """Median/IQR standardization, fit once at training time (spec §5.3)."""
    median: np.ndarray
    iqr: np.ndarray

    def transform(self, X: np.ndarray) -> np.ndarray:
        iqr_safe = np.where(self.iqr == 0, 1.0, self.iqr)
        return (X - self.median) / iqr_safe


def fit_robust_scaler(X: np.ndarray) -> RobustScalerParams:
    median = np.median(X, axis=0)
    q75 = np.percentile(X, 75, axis=0)
    q25 = np.percentile(X, 25, axis=0)
    return RobustScalerParams(median=median, iqr=q75 - q25)


class DriftModel:
    """Cold-start-safe: an unfitted DriftModel's `score()` always returns
    0.0, matching spec §5.3 ("until a cohort model exists, drift = 0")."""

    def __init__(self):
        self.scaler: Optional[RobustScalerParams] = None
        self.if_model: Optional[IsolationForest] = None
        self.ocsvm_model: Optional[OneClassSVM] = None
        self.if_score_min = 0.0
        self.if_score_max = 1.0
        self.ocsvm_score_min = 0.0
        self.ocsvm_score_max = 1.0
        self.n_samples = 0
        self.feature_names = list(FEATURE_NAMES)

    @property
    def is_fitted(self) -> bool:
        return self.if_model is not None

    def fit(self, X: np.ndarray) -> None:
        if X.shape[0] < MIN_TRAINING_SAMPLES:
            raise ValueError(f"need at least {MIN_TRAINING_SAMPLES} samples to fit a drift model, got {X.shape[0]}")

        self.scaler = fit_robust_scaler(X)
        Xs = self.scaler.transform(X)

        self.if_model = IsolationForest(**IF_PARAMS).fit(Xs)
        if_raw = -self.if_model.score_samples(Xs)  # higher = more anomalous
        self.if_score_min, self.if_score_max = float(if_raw.min()), float(if_raw.max())

        self.ocsvm_model = OneClassSVM(**OCSVM_PARAMS).fit(Xs)
        ocsvm_raw = -self.ocsvm_model.decision_function(Xs)  # higher = more anomalous
        self.ocsvm_score_min, self.ocsvm_score_max = float(ocsvm_raw.min()), float(ocsvm_raw.max())

        self.n_samples = int(X.shape[0])

    @staticmethod
    def _normalize(value: float, lo: float, hi: float) -> float:
        if hi <= lo:
            return 0.0
        return float(np.clip((value - lo) / (hi - lo), 0.0, 1.0))

    def score(self, x: list[float]) -> float:
        if not self.is_fitted:
            return 0.0
        Xs = self.scaler.transform(np.array([x], dtype=float))
        if_raw = float(-self.if_model.score_samples(Xs)[0])
        ocsvm_raw = float(-self.ocsvm_model.decision_function(Xs)[0])
        if_norm = self._normalize(if_raw, self.if_score_min, self.if_score_max)
        ocsvm_norm = self._normalize(ocsvm_raw, self.ocsvm_score_min, self.ocsvm_score_max)
        return 0.5 * if_norm + 0.5 * ocsvm_norm

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump(self, f)

    @staticmethod
    def load(path: str) -> "DriftModel":
        with open(path, "rb") as f:
            return pickle.load(f)


class DriftModelRegistry:
    """Hot-reloads per-cohort DriftModel objects from the `tmq:models:meta`
    Redis hash (spec §4.4) whenever the trainer publishes a new version, with
    no worker restart required (spec §5.3: "worker hot-reloads on meta-hash
    change")."""

    def __init__(self, redis_client):
        self.r = redis_client
        self._models: dict[str, DriftModel] = {}
        self._trained_at: dict[str, float] = {}

    def get(self, cohort: str) -> DriftModel:
        meta_raw = self.r.hget("tmq:models:meta", cohort)
        if not meta_raw:
            return self._models.get(cohort, DriftModel())
        meta = json.loads(meta_raw)
        if self._trained_at.get(cohort) != meta.get("trained_at"):
            try:
                self._models[cohort] = DriftModel.load(meta["path"])
                self._trained_at[cohort] = meta.get("trained_at")
            except (FileNotFoundError, EOFError, pickle.UnpicklingError):
                pass
        return self._models.get(cohort, DriftModel())
