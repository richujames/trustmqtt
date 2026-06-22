import time
import random
import paho.mqtt.client as mqtt

class BaseDevice:
    def __init__(self, client_id, host='localhost', port=1883, keepalive=60, clean_session=True):
        self.client_id = client_id
        self.host = host
        self.port = port
        self.keepalive = keepalive
        
        # We explicitly set clean_session as it's a key behavioral feature
        self.client = mqtt.Client(client_id=self.client_id, clean_session=clean_session)
        self._running = False

    def connect(self):
        self.client.connect(self.host, self.port, keepalive=self.keepalive)
        self.client.loop_start()

    def disconnect(self):
        self._running = False
        self.client.disconnect()
        self.client.loop_stop()

    def run(self, count=None):
        """To be implemented by subclasses to define specific publish/subscribe behavior"""
        pass
