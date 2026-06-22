from typing import Dict, Any, Optional
from .feature_schema import MessageFeatures

_last_publish_time: Dict[str, int] = {}

def process_message_event(evt: dict) -> Optional[MessageFeatures]:
    """
    Processes PUBLISH events.
    Returns MessageFeatures populated with statistics like inter-arrival time.
    """
    if evt['event_type'] != 'PUBLISH':
        return None
        
    client_id = evt['client_id']
    ts = evt['timestamp_ms']
    
    inter_arrival = 0
    if client_id in _last_publish_time:
        inter_arrival = ts - _last_publish_time[client_id]
        if inter_arrival < 0:
            inter_arrival = 0
            
    _last_publish_time[client_id] = ts
    
    pub_data = evt.get('publish', {})
    topic = pub_data.get('topic', '')
    
    # Calculate topic depth (e.g. "a/b/c" -> 3)
    topic_depth = len(topic.split('/')) if topic else 0
    
    return MessageFeatures(
        payload_size=pub_data.get('payload_size_bytes', 0),
        topic_depth=topic_depth,
        qos=pub_data.get('qos', 0),
        retain=1 if pub_data.get('retain') else 0,
        dup=1 if pub_data.get('dup') else 0,
        inter_arrival_time_ms=inter_arrival
    )
