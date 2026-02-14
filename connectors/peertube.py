import requests,sqlite3,time,os,json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB="identity.db"

# Use a working default instance
INSTANCE=os.getenv("PEERTUBE_INSTANCE","https://framatube.org").rstrip("/")

API_BASE=INSTANCE+"/api/v1"

HEADERS={
    "User-Agent":"SegmentoCollector/1.0",
    "Accept":"application/json"
}


# ---------------- Utils ----------------

def safe_json(obj):
    try:
        return json.dumps(obj,ensure_ascii=False)
    except:
        return "{}"


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(path,params=None):

    url=API_BASE+path

    try:
        r=requests.get(url,headers=HEADERS,params=params,timeout=25)

        print("PEERTUBE URL:",r.url)
        print("PEERTUBE STATUS:",r.status_code)

        if r.status_code==200:
            return r.json()

        print("PEERTUBE BODY:",r.text[:200])

    except Exception as e:
        print("PEERTUBE ERROR:",e)

    return None


# ---------------- State ----------------

def get_last_time(uid):

    con=db()
    cur=con.cursor()

    cur.execute("""
    SELECT last_published_at
    FROM peertube_state
    WHERE uid=?
    """,(uid,))

    row=cur.fetchone()
    con.close()

    return row[0] if row and row[0] else "1970-01-01T00:00:00Z"


def save_last_time(uid,ts):

    con=db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO peertube_state
    (uid,instance,last_published_at)
    VALUES(?,?,?)
    """,(uid,INSTANCE,ts))

    con.close()


# ---------------- Normalizer ----------------

def get_data(res):

    if not isinstance(res,dict):
        return []

    data=res.get("data")

    if isinstance(data,list):
        return data

    return []


# ---------------- Inserts ----------------

def insert_videos(uid,rows):

    if not rows:
        return

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for v in rows:

        ch=v.get("channel") or {}

        data.append((
            uid,
            INSTANCE,
            v.get("uuid"),
            v.get("name"),
            v.get("description"),
            v.get("duration"),
            v.get("views"),
            v.get("likes"),
            v.get("dislikes"),
            v.get("publishedAt"),
            ch.get("name"),
            v.get("url"),
            str(v.get("category")),
            str(v.get("language")),
            safe_json(v),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO peertube_videos
    (uid,instance,video_id,name,description,duration,views,likes,dislikes,
     published_at,channel_name,url,category,language,raw_json,fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


def insert_channels(uid,rows):

    if not rows:
        return

    con=db()
    cur=con.cursor()

    now=datetime.utcnow().isoformat()

    data=[]

    for c in rows:

        data.append((
            uid,
            INSTANCE,
            c.get("uuid"),
            c.get("name"),
            c.get("displayName"),
            c.get("followersCount"),
            c.get("url"),
            safe_json(c),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO peertube_channels
    (uid,instance,channel_id,name,display_name,followers,url,raw_json,fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


# ---------------- Fetchers ----------------

def fetch_videos(params):

    res=safe_get("/videos",params)

    return get_data(res)


def fetch_channels():

    res=safe_get("/video-channels",{"count":50})

    return get_data(res)


def fetch_search(term,limit):

    res=safe_get("/search/videos",{
        "search":term,
        "count":limit
    })

    return get_data(res)


# ---------------- Main Sync ----------------

def sync_peertube(uid,limit=50):

    last_ts=get_last_time(uid)

    total_new=0


    # -------- Latest (Paginated) --------

    page=0
    new_latest=[]

    while page<3:

        latest=fetch_videos({
            "count":limit,
            "sort":"-publishedAt",
            "start":page*limit
        })

        if not latest:
            break


        for v in latest:

            ts=v.get("publishedAt")

            if ts and ts>last_ts:
                new_latest.append(v)


        page+=1

        time.sleep(2)


    if new_latest:

        insert_videos(uid,new_latest)

        newest=max(v["publishedAt"] for v in new_latest)

        save_last_time(uid,newest)

        total_new=len(new_latest)


    # -------- Trending --------

    trending=fetch_videos({
        "count":limit,
        "sort":"-trending"
    }) or []

    insert_videos(uid,trending)


    # -------- Search --------

    search_items=fetch_search("ai",limit) or []

    insert_videos(uid,search_items)


    # -------- Channels --------

    channels=fetch_channels() or []

    insert_channels(uid,channels)


    return {
        "status":"ok",
        "latest_new":total_new,
        "trending":len(trending),
        "search":len(search_items),
        "channels":len(channels)
    }