# Import library to make HTTP requests (call APIs)
import requests

# Import time library to run code at fixed intervals
import time


# Central collector endpoint (our Identity Server API)
# All external data is pushed to this URL
COLLECTOR = "http://127.0.0.1:4000/api/collect"


def fetch_repo():
    """
    This function fetches repository data from GitHub
    and sends it to our data collection platform.
    """

    # GitHub public API endpoint for CPython repository
    url = "https://api.github.com/repos/python/cpython"

    # Call GitHub API and convert response to JSON
    data = requests.get(url).json()

    # Create standardized payload for ingestion
    payload = {
        "source": "github",
        "endpoint": "/repos/python/cpython",
        "data": data
    }

    # Send payload to central collector
    requests.post(COLLECTOR, json=payload)

    # Log message for monitoring
    print("GitHub Data Stored")


# Entry point of the program
if __name__ == "__main__":

    # Run continuously (like a scheduled job)
    while True:

        # Fetch and store GitHub data
        fetch_repo()

        # Wait for 60 seconds before next fetch
        # This avoids hitting API rate limits
        time.sleep(60)   # every 1 minute