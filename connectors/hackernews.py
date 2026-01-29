import requests,time

URL="http://127.0.0.1:4000/api/collect"

def fetch():

    ids=requests.get(
      "https://hacker-news.firebaseio.com/v0/topstories.json"
    ).json()[:10]

    stories=[]

    for i in ids:
        stories.append(
          requests.get(
            f"https://hacker-news.firebaseio.com/v0/item/{i}.json"
          ).json()
        )

    data={
        "source":"hackernews",
        "endpoint":"/topstories",
        "data":stories
    }

    requests.post(URL,json=data)

    print("HN stored")


if __name__=="__main__":

    while True:
        fetch()
        time.sleep(120)