from dataclasses import dataclass
from typing import List, Optional

METADATA_SCHEMA_VERSION = '1.0'

def validate_event(evt: dict) -> bool:
    if not isinstance(evt, dict):
        return False
    if evt.get('schema_version') != METADATA_SCHEMA_VERSION:
        return False
    if 'event_type' not in evt or 'client_id' not in evt or 'timestamp_ms' not in evt:
        return False
    return True

@dataclass
class SessionFeatures:
    protocol_version: int
    clean_session: int
    keep_alive_sec: int
    session_length_ms: int
    message_count: int
    topic_subscription_count: int
    has_wildcard_subscriptions: int
    has_sys_subscriptions: int
    
    def to_vector(self) -> List[float]:
        return [
            float(self.protocol_version),
            float(self.clean_session),
            float(self.keep_alive_sec),
            float(self.session_length_ms),
            float(self.message_count),
            float(self.topic_subscription_count),
            float(self.has_wildcard_subscriptions),
            float(self.has_sys_subscriptions)
        ]

@dataclass
class MessageFeatures:
    payload_size: int
    topic_depth: int
    qos: int
    retain: int
    dup: int
    inter_arrival_time_ms: int
    
    def to_vector(self) -> List[float]:
        return [
            float(self.payload_size),
            float(self.topic_depth),
            float(self.qos),
            float(self.retain),
            float(self.dup),
            float(self.inter_arrival_time_ms)
        ]
