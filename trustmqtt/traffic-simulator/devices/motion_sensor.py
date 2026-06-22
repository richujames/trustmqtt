import time
import random
import json
from .base_device import BaseDevice

class MotionSensor(BaseDevice):
    def __init__(self, client_id, host='localhost', port=1883):
        super().__init__(client_id, host, port, keepalive=60)
        self.topic = f"sensors/motion/{client_id}"

    def run(self, count=None):
        self.connect()
        self._running = True
        sent = 0
        while self._running and (count is None or sent < count):
            # Wait for "motion" event (long irregular interval)
            time.sleep(random.uniform(10.0, 20.0))
            if not self._running:
                break
            
            # Motion sensors often send a rapid burst of messages when triggered
            burst_size = random.randint(2, 5)
            for _ in range(burst_size):
                if not self._running:
                    break
                payload = json.dumps({"type": "motion", "detected": True})
                self.client.publish(self.topic, payload, qos=0)
                sent += 1
                time.sleep(0.5)
        self.disconnect()
