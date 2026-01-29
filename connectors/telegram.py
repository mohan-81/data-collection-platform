import requests,time

URL="http://127.0.0.1:4000/api/collect"

# Example public channel
CHANNEL="https://t.me/s/pythonlang"

def fetch():

    api=f"https://tgstat.ru/en/channel/@pythonlang"

    html=requests.get(api).text

    data={
        "source":"telegram",
        "endpoint":"/public_channel",
        "data":{"page":html[:5000]}
    }

    requests.post(URL,json=data)

    print("Telegram stored")


if __name__=="__main__":

    while True:
        fetch()
        time.sleep(120)