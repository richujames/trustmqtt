import redis
import json
import time
import sys
from .config import config
from .feature_engineering.feature_schema import validate_event
from .feature_engineering.session_features import process_session_event
from .feature_engineering.message_features import process_message_event

def run_consumer():
    """Consume metadata events from Redis with robust error handling and feature processing."""
    while True:
        try:
            r = redis.Redis(
                host=config.redis_host,
                port=config.redis_port,
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=10,
                retry_on_timeout=True,
            )
            r.ping()
            print('queue_consumer: connected to redis', flush=True)

            while True:
                item = r.brpop('trustmqtt:metadata_queue', timeout=5)
                if not item:
                    continue
                _, data = item
                try:
                    evt = json.loads(data)
                except Exception as e:
                    print('Invalid JSON from plugin:', e, flush=True)
                    continue
                    
                if not validate_event(evt):
                    print('Event failed schema validation:', evt, flush=True)
                    continue
                    
                print(f"Consumed {evt['event_type']} from {evt['client_id']}", flush=True)

                # Route to appropriate feature extractors
                if evt['event_type'] == 'PUBLISH':
                    msg_features = process_message_event(evt)
                    if msg_features:
                        print(f"  -> Extracted MessageFeatures: {msg_features}", flush=True)
                        # TODO: Pass to drift scorer
                
                session_features = process_session_event(evt)
                if session_features:
                    print(f"  -> Extracted SessionFeatures: {session_features}", flush=True)
                    # TODO: Pass to drift scorer

        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError) as e:
            print(f'queue_consumer: Redis connection error: {e}. Retrying in 3s...', flush=True)
            time.sleep(3)
        except Exception as e:
            print(f'queue_consumer: unexpected error: {e}. Retrying in 5s...', flush=True)
            time.sleep(5)

if __name__ == '__main__':
    run_consumer()
