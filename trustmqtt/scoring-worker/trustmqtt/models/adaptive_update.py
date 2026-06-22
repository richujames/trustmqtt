"""Adaptive update logic for TrustMQTT anomaly detection models."""

from typing import Dict, Any


def adapt_model(existing: Dict[str, Any], update_data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply an adaptive update to the stored model."""
    existing.update(update_data)
    return existing
