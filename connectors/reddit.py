import requests
import time
import json

# Your Collector Endpoint
COLLECTOR_URL = "http://127.0.0.1:4000/api/collect"

# Reddit requires User-Agent
HEADERS = {
    "User-Agent": "SegmentoDataCollector/1.0"
}

SUBREDDIT = "python"   # change if needed


def fetch_reddit():

    try:

        url = f"https://www.reddit.com/r/{SUBREDDIT}/hot.json?limit=20"

        res = requests.get(url, headers=HEADERS, timeout=10)

        if res.status_code != 200:
            print("Reddit API Error:", res.status_code)
            return

        data = res.json()

        payload = {
            "source": "reddit",
            "endpoint": f"/r/{SUBREDDIT}/hot",
            "data": data
        }

        r = requests.post(COLLECTOR_URL, json=payload, timeout=10)

        if r.status_code == 200:
            print("Reddit Data Stored")
        else:
            print("Collector Error:", r.text)


    except Exception as e:
        print("Error:", e)


if __name__ == "__main__":

    print("Reddit Connector Started...")

    while True:

        fetch_reddit()

        # every 1 minutes
        time.sleep(60)