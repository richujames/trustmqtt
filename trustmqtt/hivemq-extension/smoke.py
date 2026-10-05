"""Live, isolated HiveMQ/Redis contract + enforcement check (no scoring worker).

Run from project root: python hivemq-extension/smoke.py
Uses unique client IDs and deletes only its own verdict keys. It does not flush Redis.
"""
import argparse
import json
import socket
import sys
import threading
import time
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt
import redis

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tmq_worker.ingest import parse_entry


def wait_for(predicate, message, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(message)


def check(args, protocol, qos):
    cid = "tmq-smoke-" + uuid.uuid4().hex[:12]
    r = redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    prior = r.xrevrange("tmq:events", count=1)
    start = prior[0][0] if prior else "0-0"
    key = f"tmq:verdictp:{cid}"
    connected, disconnected = threading.Event(), threading.Event()
    received, subscriptions, disconnect_reasons = [], [], []
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid, protocol=protocol)
    client.on_connect = lambda c, u, f, reason, props: connected.set() if not reason.is_failure else None
    def on_disconnect(_client, _userdata, flags, reason, properties):
        disconnect_reasons.append((str(flags), str(reason), str(properties)))
        disconnected.set()
    client.on_disconnect = on_disconnect
    client.on_message = lambda c, u, m: received.append(m.payload.decode())
    client.on_subscribe = lambda c, u, mid, reasons, props: subscriptions.append(reasons)
    topic = f"fleet/{cid}/telemetry"

    def verdict(level, rate=10):
        r.set(key, f"{level}|0.9|{time.time()+120}|{rate}", ex=120)
        time.sleep(0.9)

    try:
        client.connect(args.host, args.port, keepalive=30)
        client.loop_start()
        assert connected.wait(5), "HiveMQ did not accept scoped demo client"
        client.subscribe(topic, qos=qos)
        wait_for(lambda: subscriptions, "Missing SUBACK")
        assert not any(reason.is_failure for reason in subscriptions[-1])
        client.subscribe("fleet/some-other-device/#", qos=qos)
        wait_for(lambda: len(subscriptions) >= 2, "Missing baseline ACL SUBACK")
        assert any(reason.is_failure for reason in subscriptions[-1]), "Baseline namespace ACL bypassed"
        client.publish(topic, "allowed", qos=qos).wait_for_publish(3)
        wait_for(lambda: "allowed" in received, "Allowed message not delivered")

        verdict(2, 0)
        client.publish(topic, "throttled", qos=qos)
        time.sleep(0.5)
        assert "throttled" not in received, "Throttle failed"
        assert not disconnected.is_set(), f"Throttle disconnected client: {disconnect_reasons}"

        verdict(3)
        client.subscribe(f"fleet/{cid}/new", qos=qos)
        wait_for(lambda: len(subscriptions) >= 3, "Missing quarantined SUBACK")
        assert any(reason.is_failure for reason in subscriptions[-1]), "Quarantine allowed subscription"
        client.publish(topic, "quarantined", qos=qos)
        time.sleep(0.5)
        assert "quarantined" not in received
        assert not disconnected.is_set(), f"Quarantine disconnected client: {disconnect_reasons}"

        verdict(0)
        client.publish(topic, "recovered", qos=qos).wait_for_publish(3)
        wait_for(lambda: "recovered" in received, "ALLOW did not restore delivery")
        client.unsubscribe(topic)
        time.sleep(0.2)
        verdict(4)
        assert disconnected.wait(5), "KICK failed to disconnect"

        events = []
        def complete_events():
            events.clear()
            for _, fields in r.xrange("tmq:events", min="(" + start):
                e = parse_entry(fields)
                if e.client_id == cid:
                    events.append(e)
            return {"connect", "publish", "subscribe", "unsubscribe", "disconnect", "enforcement"} <= {e.event for e in events}
        wait_for(complete_events, "Missing expected broker events")
        assert all(e.broker == "hivemq" for e in events)
        print(f"PASS MQTT {protocol} QoS {qos}: delivery, throttle, quarantine, recovery, KICK and event schema")
    finally:
        client.disconnect()
        client.loop_stop()
        r.delete(key)
        r.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1885)
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()
    def broker_ready():
        try:
            with socket.create_connection((args.host, args.port), timeout=1):
                return True
        except OSError:
            return False
    wait_for(broker_ready, "HiveMQ did not start", timeout=60)
    for protocol in (mqtt.MQTTv5, mqtt.MQTTv311):
        for qos in (0, 1, 2):
            check(args, protocol, qos)
