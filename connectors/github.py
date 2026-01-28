import requests
import time

COLLECTOR = "http://127.0.0.1:4000/api/collect"

def fetch_repo():

    url = "https://api.github.com/repos/python/cpython"

    data = requests.get(url).json()

    payload = {
        "source": "github",
        "endpoint": "/repos/python/cpython",
        "data": data
    }

    requests.post(COLLECTOR, json=payload)

    print("GitHub Data Stored")


if __name__ == "__main__":

    while True:
        fetch_repo()
        time.sleep(60)   # every 1 min