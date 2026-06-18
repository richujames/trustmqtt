import time
import random
import paho.mqtt.client as mqtt

class BaseDevice:
    def __init__(self, client_id, host='localhost', port=1883, keepalive=60):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.keepalive = keepalive
        self.client = mqtt.Client(client_id=self.client_id)

    def connect(self):
        self.client.connect(self.host, self.port, keepalive=self.keepalive)

    def publish_loop(self, topic, payload_fn, interval=5, jitter=1.0, count=None):
        sent = 0
        while count is None or sent < count:
            payload = payload_fn()
            self.client.publish(topic, payload)
            time.sleep(max(0.1, interval + random.uniform(-jitter, jitter)))
            sent += 1
