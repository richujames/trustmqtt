"""Explainability utilities for TrustMQTT scoring."""

from typing import Dict, Any


def explain_score(features: Dict[str, Any], score: float) -> Dict[str, Any]:
    return {"score": score, "reason": "placeholder"}
