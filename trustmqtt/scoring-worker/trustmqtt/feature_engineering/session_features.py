import time
from typing import Dict, Any, Optional
from .feature_schema import SessionFeatures

# In-memory store for active session states for v1
# Key: client_id, Value: dict of state
_active_sessions: Dict[str, Dict[str, Any]] = {}

def process_session_event(evt: dict) -> Optional[SessionFeatures]:
    """
    Processes CONNECT, SUBSCRIBE, and DISCONNECT events.
    Returns SessionFeatures on DISCONNECT, otherwise None.
    """
    client_id = evt['client_id']
    event_type = evt['event_type']
    ts = evt['timestamp_ms']
    
    if event_type == 'CONNECT':
        conn_data = evt.get('connect', {})
        _active_sessions[client_id] = {
            'connect_time': ts,
            'protocol_version': conn_data.get('protocol_version', 4),
            'clean_session': 1 if conn_data.get('clean_session') else 0,
            'keep_alive_sec': conn_data.get('keep_alive_sec', 60),
            'message_count': 0,
            'topic_subscription_count': 0,
            'has_wildcard': 0,
            'has_sys': 0
        }
        return None
        
    elif event_type == 'SUBSCRIBE':
        if client_id in _active_sessions:
            sub_data = evt.get('subscribe', {})
            topics = sub_data.get('topics', [])
            _active_sessions[client_id]['topic_subscription_count'] += len(topics)
            for t in topics:
                filter_str = t.get('filter', '')
                if '+' in filter_str or '#' in filter_str:
                    _active_sessions[client_id]['has_wildcard'] = 1
                if filter_str.startswith('$SYS'):
                    _active_sessions[client_id]['has_sys'] = 1
        return None
        
    elif event_type == 'PUBLISH':
        if client_id in _active_sessions:
            _active_sessions[client_id]['message_count'] += 1
        return None

    elif event_type == 'DISCONNECT':
        if client_id in _active_sessions:
            session = _active_sessions.pop(client_id)
            session_length = ts - session['connect_time']
            if session_length < 0:
                session_length = 0
                
            return SessionFeatures(
                protocol_version=session['protocol_version'],
                clean_session=session['clean_session'],
                keep_alive_sec=session['keep_alive_sec'],
                session_length_ms=session_length,
                message_count=session['message_count'],
                topic_subscription_count=session['topic_subscription_count'],
                has_wildcard_subscriptions=session['has_wildcard'],
                has_sys_subscriptions=session['has_sys']
            )
            
    return None
