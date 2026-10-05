"""Observe one Windows MQTTX Desktop client through the TrustMQTT pipeline."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter

import redis


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--broker", choices=("hivemq", "flashmq"), required=True)
    parser.add_argument("--duration", type=int, default=75)
    parser.add_argument("--redis-host", default="localhost")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--expected-publishes", type=int, default=1)
    args = parser.parse_args()

    client = redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )
    client.ping()
    latest = client.xrevrange("tmq:events", count=1)
    cursor = latest[0][0] if latest else "0-0"
    deadline = time.monotonic() + args.duration
    counts: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    brokers: set[str] = set()

    print(f"Watching client '{args.client_id}' for {args.duration}s")
    print("Connect and publish from MQTTX Desktop now. Press Ctrl+C to finish early.")
    try:
        while time.monotonic() < deadline:
            for _stream, entries in client.xread({"tmq:events": cursor}, count=200, block=1000):
                for entry_id, fields in entries:
                    cursor = entry_id
                    try:
                        event = json.loads(fields["v"])
                    except (KeyError, json.JSONDecodeError):
                        continue
                    if event.get("client_id") != args.client_id:
                        continue
                    kind = str(event.get("event", "unknown"))
                    counts[kind] += 1
                    if event.get("topic"):
                        topics[str(event["topic"])] += 1
                    if event.get("broker"):
                        brokers.add(str(event["broker"]))
                    print(f"[{entry_id}] {kind:<12} topic={event.get('topic', '-')} qos={event.get('qos', '-')}")
    except KeyboardInterrupt:
        print("\nObservation stopped by user.")

    verdict = client.get(f"tmq:verdictp:{args.client_id}")
    passed = brokers == {args.broker} and counts["connect"] >= 1 and counts["publish"] >= args.expected_publishes
    print("\nTrustMQTT observation summary")
    print(f"broker adapter: {', '.join(sorted(brokers)) or 'not observed'}")
    print(f"events: {dict(sorted(counts.items()))}")
    print(f"topics: {dict(topics)}")
    print(f"latest packed verdict: {verdict or 'pending / learning'}")
    print("RESULT: PASS" if passed else "RESULT: FAIL")
    client.close()
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
