import requests,time

# Replace with your API key
API_KEY="AIzaSyAWpzegLOR3dsP-AESQwOgxvCEfrZaSkbM"

# Collector endpoint
COLLECTOR="http://127.0.0.1:4000/api/collect"

# Example channel (Google Developers)
CHANNEL_ID="UC_x5XG1OV2P6uZZ5FSM9Ttw"


def fetch_channel():

    url=(
    "https://www.googleapis.com/youtube/v3/channels"
    "?part=snippet,statistics"
    f"&id={CHANNEL_ID}"
    f"&key={API_KEY}"
    )

    res=requests.get(url)

    if res.status_code!=200:
        print("API Error:",res.text)
        return

    data=res.json()

    payload={
        "source":"youtube",
        "endpoint":"/channel",
        "data":data
    }

    r=requests.post(COLLECTOR,json=payload)

    print("YouTube Stored:",r.status_code)


if __name__=="__main__":

    while True:

        fetch_channel()

        time.sleep(120)   # 2 minutes