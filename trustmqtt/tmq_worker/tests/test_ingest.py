import json

import pytest

from tmq_worker.ingest import MalformedEvent, parse_entry


def test_parse_valid_publish_event():
    fields = {"v": json.dumps({
        "v": 1, "ts": 123.456, "event": "publish", "client_id": "dev-1",
        "topic": "a/b", "qos": 1, "retain": False, "payload_len": 10,
    })}
    evt = parse_entry(fields)
    assert evt.client_id == "dev-1"
    assert evt.event == "publish"
    assert evt.topic == "a/b"


def test_broker_metadata_is_preserved():
    fields = {"v": json.dumps({
        "v": 1, "ts": 1.0, "event": "connect", "client_id": "dev-1",
        "broker": "flashmq", "broker_id": "edge-west-1",
    })}
    evt = parse_entry(fields)
    assert evt.broker == "flashmq"
    assert evt.broker_id == "edge-west-1"


def test_parse_event_with_nested_props():
    fields = {"v": json.dumps({
        "v": 1, "ts": 1.0, "event": "publish", "client_id": "dev-1",
        "topic": "a/b", "qos": 0,
        "props": {"content_type": "application/json", "message_expiry": 30, "user_prop_count": 1},
    })}
    evt = parse_entry(fields)
    assert evt.props.content_type == "application/json"
    assert evt.props.user_prop_count == 1


def test_missing_v_field_is_malformed():
    with pytest.raises(MalformedEvent):
        parse_entry({})


def test_invalid_json_is_malformed():
    with pytest.raises(MalformedEvent):
        parse_entry({"v": "{not json"})


def test_missing_required_field_is_malformed():
    fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": "publish"})}  # no client_id
    with pytest.raises(MalformedEvent):
        parse_entry(fields)


def test_plugin_stats_without_client_id_is_accepted():
    """plugin_stats describes the broker/plugin as a whole, so it carries no
    client_id. Requiring one dead-lettered every stats event the plugin ever
    emitted, discarding its dropped_events/ring_size backpressure telemetry."""
    fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": "plugin_stats",
                               "dropped_events": 3, "ring_size": 128})}
    evt = parse_entry(fields)
    assert evt.client_id is None
    assert evt.dropped_events == 3
    assert evt.ring_size == 128


def test_client_scoped_event_still_requires_client_id():
    """The broker-level exemption must not leak to client-scoped events."""
    for event_type in ("publish", "connect", "disconnect", "subscribe"):
        fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": event_type})}
        with pytest.raises(MalformedEvent):
            parse_entry(fields)


def test_enforcement_event_is_accepted():
    """The plugin emits `enforcement` when a KICK verdict is actioned. It was
    absent from EventType, so every one of them was dead-lettered."""
    fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": "enforcement",
                               "client_id": "dev-1"})}
    evt = parse_entry(fields)
    assert evt.event == "enforcement"
    assert evt.client_id == "dev-1"


def test_unknown_event_type_is_malformed():
    fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": "not_a_real_event", "client_id": "dev-1"})}
    with pytest.raises(MalformedEvent):
        parse_entry(fields)
