import time
import json
from devices.base_device import BaseDevice

class DifferentClientLibAttack(BaseDevice):
    def __init__(self, target_client_id, host='localhost', port=1883):
        # A normal temperature sensor has keepalive=60 and clean_session=True.
        # This attacker uses a generic library default (keepalive=300, clean_session=False).
        super().__init__(target_client_id, host, port, keepalive=300, clean_session=False)

    def run(self, count=None):
        print(f"[*] ATTACK: Launching generic client lib simulation as {self.client_id}")
        self.connect()
        self._running = True
        sent = 0
        while self._running and (count is None or sent < count):
            # Publishes normally, but the connection fingerprint is wrong
            payload = json.dumps({"type": "temperature", "value": 24.0})
            self.client.publish(f"sensors/temp/{self.client_id}", payload, qos=0)
            sent += 1
            time.sleep(5.0)
        self.disconnect()
