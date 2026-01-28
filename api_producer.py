# Used to send HTTP requests
import requests

# Used for scheduling execution
import time

# Used to generate random test data
import random


# Central API ingestion endpoint (Identity Server)
URL = "http://127.0.0.1:4000/api/collect"


# Continuous data producer (simulates external SaaS/CRM systems)
while True:

    # Generate synthetic CRM-style data
    data = {
        "source": "crm_system",      # Identifies sending system
        "endpoint": "/leads",        # Logical API endpoint
        "data": {
            "lead_id": random.randint(1000,9999),
            "score": random.randint(1,100),
            "status": "new"
        }
    }

    # Push generated data to ingestion API
    res = requests.post(URL, json=data)

    # Log response for monitoring
    print("Sent:", res.json())

    # Send data every 5 seconds
    time.sleep(5)