"""Baseline modeling and feature aggregation for TrustMQTT."""

from typing import Any, Dict


class BaselineStore:
    def __init__(self):
        self.baseline: Dict[str, Any] = {}

    def update(self, features: Dict[str, Any]) -> None:
        self.baseline.update(features)

    def get(self) -> Dict[str, Any]:
        return self.baseline
