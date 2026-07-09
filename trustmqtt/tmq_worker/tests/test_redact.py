from tmq_worker.redact import mask_ip, pseudonymize, redact_incident_summary, redact_topic

SECRET_PATTERNS = ["+/credentials/#", "+/keys/#"]


def test_mask_ip_masks_to_slash_24():
    assert mask_ip("10.0.3.17") == "10.0.3.x"


def test_mask_ip_leaves_non_ipv4_alone():
    assert mask_ip("") == ""
    assert mask_ip("fe80::1") == "fe80::1"


def test_pseudonymize_is_deterministic_and_secret_dependent():
    a1 = pseudonymize("sensor-042", "secret-a")
    a2 = pseudonymize("sensor-042", "secret-a")
    b = pseudonymize("sensor-042", "secret-b")
    assert a1 == a2
    assert a1 != b
    assert a1.startswith("client-")


def test_redact_topic_matches_secret_pattern():
    # "+/credentials/#": both the wildcarded device-id segment ('+') and the
    # trailing '#' segment are redacted; only the literal "credentials"
    # segment (what makes this pattern match at all) is left visible.
    assert redact_topic("plant-a/credentials/token", SECRET_PATTERNS) == "⟦redacted⟧/credentials/⟦redacted⟧"
    assert redact_topic("plant-a/temp", SECRET_PATTERNS) == "plant-a/temp"


def test_redact_incident_summary_full_shape():
    summary = {
        "client_id": "sensor-042",
        "username": "plant-a",
        "ip": "10.0.3.17",
        "payload_sha256": "deadbeef",
        "peak_level": 3,
        "peak_score": 0.81,
        "reason": "fsm=0.9 drift=0.4 fleet=0.0",
        "fsm_diff": [{"from": "CONNECT", "to": "SUB(plant-a/credentials/+)", "learned_p": 0.001}],
    }
    out = redact_incident_summary(summary, secret="s3cr3t", secret_topic_patterns=SECRET_PATTERNS)

    assert out["client_id"].startswith("client-")
    assert out["client_id"] != "sensor-042"
    assert out["username"].startswith("client-")
    assert out["ip"] == "10.0.3.x"
    assert "payload_sha256" not in out
    assert out["peak_level"] == 3
    assert "⟦redacted⟧" in out["fsm_diff"][0]["to"]
