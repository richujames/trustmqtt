"""YAML config loader (spec §8) plus env-derived connection settings.

Thresholds/weights/windows live in tmq.yaml — never as magic numbers in
code (spec §12 rule 3). Secrets and per-deployment connection strings stay
in the environment (12-factor), not in the checked-in YAML.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Optional

import yaml


@dataclass
class LearningConfig:
    max_events: int = 2000
    max_hours: float = 24


@dataclass
class WeightsConfig:
    fsm: float = 0.45
    drift: float = 0.35
    fleet: float = 0.20


@dataclass
class ThresholdsConfig:
    watch: float = 0.30
    throttle: float = 0.50
    quarantine: float = 0.70
    kick: float = 0.85
    kick_single: float = 0.95


@dataclass
class HysteresisConfig:
    deescalate_windows: int = 3
    margin: float = 0.05


@dataclass
class FleetConfig:
    coordinated_fraction: float = 0.20
    z_trigger: float = 2.0


@dataclass
class TopicClassConfig:
    numeric_suffix_regex: str = r"^[a-z]*\d+$"


@dataclass
class RedactionConfig:
    secret_topic_patterns: list = field(default_factory=lambda: ["+/credentials/#", "+/keys/#"])


@dataclass
class LLMConfig:
    # NVIDIA NIM — OpenAI-compatible chat-completions API.
    model: str = "meta/llama-3.1-8b-instruct"
    timeout_s: float = 10
    enabled: bool = True


@dataclass
class TmqConfig:
    window_s: float = 60
    learning: LearningConfig = field(default_factory=LearningConfig)
    weights: WeightsConfig = field(default_factory=WeightsConfig)
    thresholds: ThresholdsConfig = field(default_factory=ThresholdsConfig)
    hysteresis: HysteresisConfig = field(default_factory=HysteresisConfig)
    fleet: FleetConfig = field(default_factory=FleetConfig)
    topic_class: TopicClassConfig = field(default_factory=TopicClassConfig)
    redaction: RedactionConfig = field(default_factory=RedactionConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    mode: str = "enforce"

    # Env-derived, not part of tmq.yaml:
    redis_host: str = "redis"
    redis_port: int = 6379
    database_url: str = "sqlite:///trustmqtt.db"
    nvidia_api_key: Optional[str] = None
    nvidia_base_url: str = "https://integrate.api.nvidia.com/v1"
    redaction_secret: str = "dev-only-change-me"


def load_config(path: Optional[str] = None) -> TmqConfig:
    path = path or os.environ.get("TMQ_CONFIG", "config/tmq.yaml")
    raw = {}
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

    cfg = TmqConfig(
        window_s=raw.get("window_s", 60),
        learning=LearningConfig(**raw.get("learning", {})),
        weights=WeightsConfig(**raw.get("weights", {})),
        thresholds=ThresholdsConfig(**raw.get("thresholds", {})),
        hysteresis=HysteresisConfig(**raw.get("hysteresis", {})),
        fleet=FleetConfig(**raw.get("fleet", {})),
        topic_class=TopicClassConfig(**raw.get("topic_class", {})),
        redaction=RedactionConfig(**raw.get("redaction", {})),
        llm=LLMConfig(**raw.get("llm", {})),
        mode=raw.get("mode", "enforce"),
    )

    cfg.redis_host = os.environ.get("REDIS_HOST", cfg.redis_host)
    cfg.redis_port = int(os.environ.get("REDIS_PORT", cfg.redis_port))
    cfg.database_url = os.environ.get("DATABASE_URL", cfg.database_url)
    cfg.nvidia_api_key = os.environ.get("NVIDIA_API_KEY", cfg.nvidia_api_key)
    cfg.nvidia_base_url = os.environ.get("NVIDIA_BASE_URL", cfg.nvidia_base_url)
    cfg.redaction_secret = os.environ.get("REDACTION_SECRET", cfg.redaction_secret)

    return cfg
