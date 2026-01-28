# Import HTTP request library
import requests

# Import time module for scheduling
import time

# Import JSON for safe data handling (optional use)
import json


# Central collector endpoint
# All Reddit data will be pushed here
COLLECTOR_URL = "http://127.0.0.1:4000/api/collect"


# Reddit requires User-Agent to prevent bot blocking
# This identifies our application
HEADERS = {
    "User-Agent": "SegmentoDataCollector/1.0"
}


# Subreddit to collect data from
# Can be changed to any public subreddit
SUBREDDIT = "python"


def fetch_reddit():
    """
    This function fetches trending posts from Reddit
    and pushes them to the data platform.
    """

    try:

        # Build Reddit API URL for hot posts
        url = f"https://www.reddit.com/r/{SUBREDDIT}/hot.json?limit=20"

        # Call Reddit API with headers
        res = requests.get(
            url,
            headers=HEADERS,
            timeout=10   # avoid hanging if API is slow
        )

        # Check if API call is successful
        if res.status_code != 200:
            print("Reddit API Error:", res.status_code)
            return

        # Convert response into JSON
        data = res.json()

        # Create standardized payload
        payload = {
            "source": "reddit",                     # Data source
            "endpoint": f"/r/{SUBREDDIT}/hot",       # API route
            "data": data                            # Raw response
        }

        # Send payload to collector
        r = requests.post(
            COLLECTOR_URL,
            json=payload,
            timeout=10
        )

        # Check if data is stored successfully
        if r.status_code == 200:
            print("Reddit Data Stored")
        else:
            print("Collector Error:", r.text)


    # Handle network / runtime errors safely
    except Exception as e:
        print("Error:", e)


# Entry point
if __name__ == "__main__":

    print("Reddit Connector Started...")


    # Continuous polling loop
    while True:

        # Fetch and push Reddit data
        fetch_reddit()

        # Wait before next execution
        # Prevents API abuse and rate limit issues
        time.sleep(60)   # every 1 minute