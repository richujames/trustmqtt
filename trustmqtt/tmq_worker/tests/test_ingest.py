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


def test_unknown_event_type_is_malformed():
    fields = {"v": json.dumps({"v": 1, "ts": 1.0, "event": "not_a_real_event", "client_id": "dev-1"})}
    with pytest.raises(MalformedEvent):
        parse_entry(fields)
