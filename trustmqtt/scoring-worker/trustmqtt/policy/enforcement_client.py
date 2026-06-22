"""Enforcement client for TrustMQTT policy actions."""

from typing import Dict, Any


def enforce_action(action: str, target: Dict[str, Any]) -> bool:
    return True
