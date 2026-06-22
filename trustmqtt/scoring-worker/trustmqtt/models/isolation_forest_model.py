"""Isolation forest model placeholder for TrustMQTT anomaly detection."""

from typing import Any, Dict


class IsolationForestModel:
    def __init__(self):
        self.model = None

    def fit(self, features: Dict[str, Any]) -> None:
        self.model = features

    def score(self, features: Dict[str, Any]) -> float:
        return 0.0
