import redis
import json
from .config import config

def run_consumer():
    r = redis.Redis(host=config.redis_host, port=config.redis_port, decode_responses=True)
    print('queue_consumer: connected to redis')
    while True:
        item = r.brpop('trustmqtt:metadata_queue', timeout=5)
        if not item:
            continue
        _, data = item
        try:
            evt = json.loads(data)
        except Exception as e:
            print('Invalid JSON from plugin:', e)
            continue
        print('Consumed event:', evt)

if __name__ == '__main__':
    run_consumer()
