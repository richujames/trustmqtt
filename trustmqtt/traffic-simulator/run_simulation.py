from devices.base_device import BaseDevice

if __name__ == '__main__':
    dev = BaseDevice('sim-device-1', host='localhost')
    dev.connect()
    dev.publish_loop('sensors/temp', lambda: '23.5', interval=2, count=10)
