import requests,time

URL="http://127.0.0.1:4000/api/collect"

INSTANCE="https://mastodon.social"

def fetch():

    api=f"{INSTANCE}/api/v1/timelines/public"

    res=requests.get(api).json()

    data={
        "source":"mastodon",
        "endpoint":"/public_timeline",
        "data":res
    }

    requests.post(URL,json=data)

    print("Mastodon stored")


if __name__=="__main__":

    while True:
        fetch()
        time.sleep(120)