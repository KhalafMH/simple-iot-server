import random
from datetime import datetime, timezone
from sys import stderr, argv
from time import sleep

import requests
from requests import RequestException

if len(argv) < 2:
    print(f"usage: {argv[0]} <sensor_id>")
    exit(1)
sensor_id = argv[1]

while True:
    value = random.random() * 50.0 - 25.0
    if value < -20 or value > 15:
        alert = True
    else:
        alert = False
    reading = {
        "id": sensor_id,
        "type": "temperature",
        "value": value,
        "alert": alert,
        "timestamp": datetime.now().astimezone(timezone.utc).isoformat()
    }
    print(f"sending reading: {reading}")
    try:
        requests.request("post", "http://localhost:8080/sensor-readings", json=reading)
    except RequestException as e:
        print("request failed", file=stderr)

    sleep(1.0)
