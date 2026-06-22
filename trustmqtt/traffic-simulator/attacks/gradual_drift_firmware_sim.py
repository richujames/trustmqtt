import time
import json
import random
from devices.base_device import BaseDevice

class GradualDriftAttack(BaseDevice):
    def __init__(self, target_client_id, host='localhost', port=1883):
        super().__init__(target_client_id, host, port, keepalive=60)
        self.topic = f"sensors/temp/{self.client_id}"

    def run(self, count=None):
        print(f"[*] ATTACK: Launching gradual firmware drift as {self.client_id}")
        self.connect()
        self._running = True
        sent = 0
        base_interval = 5.0
        payload_size_multiplier = 1
        
        while self._running and (count is None or sent < count):
            # Payload slowly gets larger (simulating new firmware adding more JSON fields)
            payload_data = {"type": "firmware_update", "metadata": "X" * (10 * payload_size_multiplier)}
            self.client.publish(self.topic, json.dumps(payload_data), qos=1)
            
            # Interval slowly gets smaller
            time.sleep(max(0.5, base_interval))
            
            base_interval -= 0.1
            payload_size_multiplier += 1
            sent += 1
            
        self.disconnect()
