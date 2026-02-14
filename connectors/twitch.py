import requests
import sqlite3
import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

DB = "identity.db"

CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
BASE = "https://api.twitch.tv/helix"


# ---------------- DB ----------------

def db():
    return sqlite3.connect(DB, timeout=30)


# ---------------- AUTH ----------------

_token = None
_token_expiry = None


def get_token():

    global _token, _token_expiry

    if _token and _token_expiry > datetime.datetime.now():
        return _token

    if not CLIENT_ID or not CLIENT_SECRET:
        raise Exception("Twitch credentials missing")

    r = requests.post(TOKEN_URL, params={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    })

    if r.status_code != 200:
        raise Exception(r.text)

    data = r.json()

    _token = data["access_token"]
    _token_expiry = datetime.datetime.now() + datetime.timedelta(
        seconds=data["expires_in"] - 60
    )

    return _token


# ---------------- API ----------------

def twitch_get(path, params=None):

    token = get_token()

    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {token}"
    }

    url = f"{BASE}/{path}"

    r = requests.get(url, headers=headers, params=params, timeout=20)

    if r.status_code != 200:
        raise Exception(r.text)

    return r.json()["data"]


# ---------------- SYNC USER ----------------

def sync_user(uid, username):

    users = twitch_get("users", {
        "login": username
    })

    if not users:
        raise Exception("User not found")

    u = users[0]

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    cur.execute("""
    INSERT OR REPLACE INTO twitch_users
    (uid, twitch_id, login, display_name,
     description, followers, view_count,
     raw_json, fetched_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        uid,
        u["id"],
        u["login"],
        u["display_name"],
        u["description"],
        None,
        u["view_count"],
        json.dumps(u),
        now
    ))

    con.commit()
    con.close()

    return {"user": u["display_name"]}


# ---------------- SYNC STREAM ----------------

def sync_stream(uid, username):

    users = twitch_get("users", {
        "login": username
    })

    if not users:
        raise Exception("User not found")

    user_id = users[0]["id"]

    streams = twitch_get("streams", {
        "user_id": user_id
    })

    if not streams:
        return {"stream": "offline"}

    s = streams[0]

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    cur.execute("""
    INSERT INTO twitch_streams
    (uid, twitch_id, title, game_name,
     viewer_count, started_at,
     raw_json, fetched_at)
    VALUES (?,?,?,?,?,?,?,?)
    """, (
        uid,
        user_id,
        s["title"],
        s["game_name"],
        s["viewer_count"],
        s["started_at"],
        json.dumps(s),
        now
    ))

    con.commit()
    con.close()

    return {"stream": "live"}


# ---------------- SYNC VIDEOS ----------------

def sync_videos(uid, username, limit=10):

    users = twitch_get("users", {
        "login": username
    })

    if not users:
        raise Exception("User not found")

    user_id = users[0]["id"]

    videos = twitch_get("videos", {
        "user_id": user_id,
        "first": limit
    })

    con = db()
    cur = con.cursor()

    now = datetime.datetime.now().isoformat()

    count = 0

    for v in videos:

        cur.execute("""
        INSERT OR REPLACE INTO twitch_videos
        (uid, twitch_id, video_id,
         title, views, duration,
         created_at, raw_json, fetched_at)
        VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            user_id,
            v["id"],
            v["title"],
            v["view_count"],
            v["duration"],
            v["created_at"],
            json.dumps(v),
            now
        ))

        count += 1

    con.commit()
    con.close()

    return {"videos": count}