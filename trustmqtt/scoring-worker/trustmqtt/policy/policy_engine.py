"""Policy engine for TrustMQTT."""

from typing import Dict, Any


class PolicyEngine:
    def evaluate(self, features: Dict[str, Any]) -> bool:
        return True
