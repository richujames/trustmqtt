import time
import random
import json
from .base_device import BaseDevice

class DoorLock(BaseDevice):
    def __init__(self, client_id, host='localhost', port=1883):
        # Locks typically have longer keep-alives and listen for commands
        super().__init__(client_id, host, port, keepalive=120)
        self.topic = f"locks/{client_id}/state"
        self.cmd_topic = f"locks/{client_id}/cmd"

    def connect(self):
        super().connect()
        # They establish a baseline of subscribing to command topics
        self.client.subscribe(self.cmd_topic, qos=1)

    def run(self, count=None):
        self.connect()
        self._running = True
        sent = 0
        while self._running and (count is None or sent < count):
            # Locks publish irregularly, event-driven
            time.sleep(random.uniform(5.0, 15.0))
            if not self._running:
                break
            payload = json.dumps({"type": "door_lock", "state": random.choice(["locked", "unlocked"])})
            # QoS 1 because state changes are critical
            self.client.publish(self.topic, payload, qos=1)
            sent += 1
        self.disconnect()
