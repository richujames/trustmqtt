import time
from devices.base_device import BaseDevice

class ReconWildcardAttack(BaseDevice):
    def __init__(self, target_client_id, host='localhost', port=1883):
        # Pretend to be a publish-only device that suddenly subscribes to everything
        super().__init__(target_client_id, host, port, keepalive=60)

    def run(self, count=None):
        print(f"[*] ATTACK: Launching recon wildcard subscribe as {self.client_id}")
        self.connect()
        # Anomalous subscription behavior
        self.client.subscribe("#", qos=0)
        self.client.subscribe("$SYS/#", qos=0)
        
        # Keep connection open to receive messages
        self._running = True
        time.sleep(5)
        self.disconnect()
