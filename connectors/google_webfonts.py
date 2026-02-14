import os
import requests
import sqlite3
import datetime
import json
import time

from destinations.destination_router import push_to_destination

SOURCE = "webfonts"

DB = "identity.db"

WEBFONTS_URL = "https://webfonts.googleapis.com/v1/webfonts"


# ---------------- DB CONNECT ---------------- #

def db_connect(retries=5, delay=0.5):

    last_err = None

    for i in range(retries):
        try:
            con = sqlite3.connect(DB, timeout=30)
            con.text_factory = str
            return con
        except sqlite3.OperationalError as e:
            last_err = e
            time.sleep(delay * (i + 1))

    raise last_err


# ---------------- INIT TABLE ---------------- #

def init_webfonts_table():

    con = db_connect()
    cur = con.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS google_webfonts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        family TEXT,
        category TEXT,
        variants TEXT,
        subsets TEXT,
        files TEXT,
        last_modified TEXT,
        raw_json TEXT,
        fetched_at TEXT
    )
    """)

    con.commit()
    con.close()


# ---------------- MAIN SYNC ---------------- #

def sync_webfonts(sync_type="incremental"):

    uid = get_connected_user()

    if not uid:
        return {
            "status": "error",
            "message": "WebFonts connector not connected"
        }

    init_webfonts_table()

    api_key = os.getenv("GOOGLE_API_KEY")

    if not api_key:
        return {
            "status": "error",
            "message": "GOOGLE_API_KEY missing"
        }

    params = {
        "key": api_key,
        "sort": "POPULARITY"
    }

    r = requests.get(WEBFONTS_URL, params=params, timeout=30)

    if r.status_code != 200:
        return {
            "status": "error",
            "message": r.text
        }

    data = r.json()
    fonts = data.get("items", [])

    con = db_connect()
    cur = con.cursor()

    now = datetime.datetime.utcnow().isoformat()

    rows_for_destination = []

    for f in fonts:

        cur.execute("""
            INSERT OR REPLACE INTO google_webfonts
            (family, category, variants, subsets, files, last_modified, raw_json, fetched_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (
            f.get("family"),
            f.get("category"),
            json.dumps(f.get("variants")),
            json.dumps(f.get("subsets")),
            json.dumps(f.get("files")),
            f.get("lastModified"),
            json.dumps(f, ensure_ascii=False),
            now
        ))

        rows_for_destination.append({
            "family": f.get("family"),
            "category": f.get("category"),
            "variants": json.dumps(f.get("variants")),
            "subsets": json.dumps(f.get("subsets")),
            "files": json.dumps(f.get("files")),
            "last_modified": f.get("lastModified")
        })

    con.commit()
    con.close()

    # ---------- Push to destination ----------
    dest = get_active_destination(uid)

    if dest and rows_for_destination:
        push_to_destination(dest, SOURCE, rows_for_destination)

    return {
        "status": "success",
        "fonts": len(rows_for_destination)
    }

def get_connected_user():
    con = db_connect()
    cur = con.cursor()

    cur.execute("""
        SELECT uid
        FROM google_connections
        WHERE source=? AND enabled=1
        LIMIT 1
    """, (SOURCE,))

    row = cur.fetchone()
    con.close()

    return row[0] if row else None

def get_active_destination(uid):
    con = db_connect()
    cur = con.cursor()

    cur.execute("""
        SELECT
            dest_type,
            host,
            port,
            username,
            password,
            database_name
        FROM destination_configs
        WHERE uid=? AND source=?
        ORDER BY id DESC
        LIMIT 1
    """, (uid, SOURCE))

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "type": row[0],
        "host": row[1],
        "port": row[2],
        "username": row[3],
        "password": row[4],
        "database_name": row[5]
    }
