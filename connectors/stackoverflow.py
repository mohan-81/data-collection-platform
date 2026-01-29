import requests,time

URL="http://127.0.0.1:4000/api/collect"

def fetch():

    api="https://api.stackexchange.com/2.3/questions?order=desc&sort=activity&site=stackoverflow"

    res=requests.get(api).json()

    data={
        "source":"stackoverflow",
        "endpoint":"/questions",
        "data":res
    }

    requests.post(URL,json=data)

    print("SO stored")


if __name__=="__main__":

    while True:
        fetch()
        time.sleep(120)