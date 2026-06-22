import time
import random
import json
from .base_device import BaseDevice

class TemperatureSensor(BaseDevice):
    def __init__(self, client_id, host='localhost', port=1883):
        # Temperature sensors typically use QoS 0, steady publish intervals
        super().__init__(client_id, host, port, keepalive=60)
        self.topic = f"sensors/temp/{client_id}"

    def run(self, count=None):
        self.connect()
        self._running = True
        sent = 0
        while self._running and (count is None or sent < count):
            payload = json.dumps({"type": "temperature", "value": round(random.uniform(20.0, 25.0), 2)})
            self.client.publish(self.topic, payload, qos=0)
            sent += 1
            # Publish roughly every 5 seconds with slight jitter
            time.sleep(5.0 + random.uniform(-0.5, 0.5))
        self.disconnect()
