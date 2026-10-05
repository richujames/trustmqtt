import paho.mqtt.publish as publish


def main() -> None:
    publish.single(
        topic="fleet/demo-client/temperature",
        payload="25.4",
        hostname="localhost",
        port=1883,
        client_id="demo-client",
    )


if __name__ == "__main__":
    main()
