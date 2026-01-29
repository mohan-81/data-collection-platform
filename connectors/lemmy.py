import requests,time

URL="http://127.0.0.1:4000/api/collect"

INSTANCE="https://lemmy.world"

def fetch():

    api=f"{INSTANCE}/api/v3/post/list?type_=All&sort=Active"

    res=requests.get(api).json()

    data={
        "source":"lemmy",
        "endpoint":"/posts",
        "data":res
    }

    requests.post(URL,json=data)

    print("Lemmy stored")


if __name__=="__main__":

    while True:
        fetch()
        time.sleep(120)