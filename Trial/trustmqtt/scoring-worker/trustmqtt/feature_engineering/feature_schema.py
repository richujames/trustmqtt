# Canonical schema and simple validator for incoming metadata events

METADATA_SCHEMA_VERSION = '1.0'

# Minimal helper to check top-level keys; full validation should be added.
def validate_event(evt: dict) -> bool:
    if not isinstance(evt, dict):
        return False
    if evt.get('schema_version') != METADATA_SCHEMA_VERSION:
        return False
    if 'event_type' not in evt or 'client_id' not in evt or 'timestamp_ms' not in evt:
        return False
    return True
