import requests
import time
import random

URL = "http://127.0.0.1:4000/api/collect"

while True:

    data = {
        "source": "crm_system",
        "endpoint": "/leads",
        "data": {
            "lead_id": random.randint(1000,9999),
            "score": random.randint(1,100),
            "status": "new"
        }
    }

    res = requests.post(URL, json=data)

    print("Sent:", res.json())

    time.sleep(5)