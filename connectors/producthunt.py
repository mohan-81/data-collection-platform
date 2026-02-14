import requests
import sqlite3
import time
import os
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

DB = "identity.db"

API_URL = "https://api.producthunt.com/v2/api/graphql"

TOKEN = os.getenv("PRODUCTHUNT_TOKEN")


# ---------------- DB Helper ----------------

def db():

    con = sqlite3.connect(
        DB,
        timeout=90,
        check_same_thread=False,
        isolation_level=None
    )

    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")

    return con


# ---------------- Token Helper ----------------

def get_headers():

    if not TOKEN:
        raise Exception("PRODUCTHUNT_TOKEN not set in .env")

    return {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json"
    }


# ---------------- State ----------------

def get_last_time(uid):

    con = db()
    cur = con.cursor()

    cur.execute("""
    SELECT last_post_time
    FROM producthunt_state
    WHERE uid=?
    """, (uid,))

    row = cur.fetchone()

    con.close()

    if row and row[0]:
        return row[0]

    return "1970-01-01T00:00:00Z"


def save_last_time(uid, ts):

    con = db()
    cur = con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO producthunt_state
    (uid, last_post_time)
    VALUES (?,?)
    """, (uid, ts))

    con.close()


# ---------------- HTTP ----------------

def safe_post(query):

    try:

        r = requests.post(
            API_URL,
            json={"query": query},
            headers=get_headers(),
            timeout=15
        )

        if r.status_code == 200:
            return r.json()

        if r.status_code == 429:
            time.sleep(60)

    except Exception:
        time.sleep(5)

    return None


# ---------------- Batch Inserts ----------------

def insert_posts(uid, posts):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    rows = []


    for p in posts:

        topics = [
            t["node"]["name"]
            for t in p.get("topics", {}).get("edges", [])
        ]

        makers = [
            m.get("name")
            for m in p.get("makers", [])
        ]


        rows.append((
            uid,

            p.get("id"),

            p.get("name"),
            p.get("tagline"),

            p.get("votesCount"),
            p.get("commentsCount"),

            p.get("createdAt"),
            p.get("url"),

            ",".join(topics),
            ",".join(makers),

            str(p),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO producthunt_posts (

        uid,

        post_id,

        name,
        tagline,

        votes,
        comments,

        created_at,
        url,

        topics,
        makers,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

    con.close()


def insert_topics(uid, topics):

    con = db()
    cur = con.cursor()

    now = datetime.utcnow().isoformat()

    rows = []


    for t in topics:

        rows.append((
            uid,

            t.get("id"),

            t.get("name"),
            t.get("slug"),

            t.get("followersCount"),

            str(t),

            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO producthunt_topics (

        uid,

        topic_id,

        name,
        slug,

        followers,

        raw_json,
        fetched_at
    )
    VALUES (?,?,?,?,?,?,?)
    """, rows)

    con.close()


# ---------------- Main Sync ----------------

def sync_producthunt(uid, limit=30):

    last_time = get_last_time(uid)


    # -------- Posts --------

    posts_q = f"""
    {{
      posts(first: {limit}) {{
        nodes {{
          id
          name
          tagline
          votesCount
          commentsCount
          createdAt
          url
          topics {{
            edges {{
              node {{
                name
              }}
            }}
          }}
          makers {{
            name
            username
          }}
        }}
      }}
    }}
    """


    posts_data = safe_post(posts_q)


    if not posts_data:
        return {
            "status": "error",
            "msg": "Failed to fetch posts"
        }


    posts = posts_data.get("data", {}) \
                      .get("posts", {}) \
                      .get("nodes", [])


    # Incremental filter
    new_posts = []


    for p in posts:

        if p.get("createdAt") > last_time:
            new_posts.append(p)


    if new_posts:
        insert_posts(uid, new_posts)

        latest = max(p["createdAt"] for p in new_posts)

        save_last_time(uid, latest)


    # -------- Topics --------

    topics_q = """
    {
      topics(first: 20) {
        nodes {
          id
          name
          slug
          followersCount
        }
      }
    }
    """


    topics_data = safe_post(topics_q)


    if topics_data:

        topics = topics_data.get("data", {}) \
                            .get("topics", {}) \
                            .get("nodes", [])

        if topics:
            insert_topics(uid, topics)


    return {
        "status": "ok",
        "posts_inserted": len(new_posts),
        "topics_inserted": len(topics) if topics_data else 0
    }