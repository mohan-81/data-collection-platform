import requests
import sqlite3
import datetime
import json
import os
import time

from dotenv import load_dotenv
from destinations.destination_router import push_to_destination

# Load .env
load_dotenv()

SOURCE = "pagespeed"
DB = "identity.db"

BASE_URL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed"


# ---------------- CONNECTION HELPERS ---------------- #
def get_connected_user():
    con = sqlite3.connect(DB)
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
    con = sqlite3.connect(DB)
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


# ---------------- CORE API CALL ---------------- #

def fetch_pagespeed(url, strategy, categories):

    params = {
        "url": url,
        "strategy": strategy
    }

    for c in categories:
        params.setdefault("category", []).append(c)

    api_key = os.getenv("GOOGLE_API_KEY")

    if api_key:
        params["key"] = api_key

    for i in range(5):
        try:
            r = requests.get(BASE_URL, params=params, timeout=60)

            if r.status_code == 200:
                return r.json()

            if r.status_code == 429:
                wait = 5 * (i + 1)
                print(f"[PageSpeed] Rate limited. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            raise Exception(f"{r.status_code}: {r.text}")

        except Exception as e:
            print(f"[PageSpeed Error] {e}")
            if i == 4:
                raise e
            time.sleep(3)


# ---------------- SCORE EXTRACTION ---------------- #

def extract_scores(data):

    cats = data.get("lighthouseResult", {}).get("categories", {})

    def score(name):
        v = cats.get(name, {}).get("score")
        return float(v) * 100 if v is not None else None

    return {
        "performance": score("performance"),
        "seo": score("seo"),
        "accessibility": score("accessibility"),
        "best-practices": score("best-practices"),
        "pwa": score("pwa")
    }


# ---------------- MAIN SYNC ---------------- #

def sync_pagespeed(url, sync_type="incremental"):

    uid = get_connected_user()

    if not uid:
        return {
            "status": "error",
            "message": "PageSpeed connector not connected"
        }

    strategies = ["mobile", "desktop"]
    categories = [
        "performance",
        "seo",
        "accessibility",
        "best-practices",
        "pwa"
    ]

    con = sqlite3.connect(DB)
    cur = con.cursor()

    rows_for_destination = []

    now = datetime.datetime.utcnow().isoformat()
    count = 0

    for strat in strategies:

        print(f"[PageSpeed] Fetching {url} ({strat})")

        data = fetch_pagespeed(url, strat, categories)
        scores = extract_scores(data)

        # Insert locally
        cur.execute("""
            INSERT INTO google_pagespeed
            (uid, url, strategy, categories,
            performance_score, seo_score, accessibility_score,
            best_practices_score, pwa_score,
            raw_response, fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            uid,
            url,
            strat,
            ",".join(categories),
            scores["performance"],
            scores["seo"],
            scores["accessibility"],
            scores["best-practices"],
            scores["pwa"],
            json.dumps(data),
            now
        ))

        # Prepare for destination
        rows_for_destination.append({
            "url": url,
            "strategy": strat,
            "performance_score": scores["performance"],
            "seo_score": scores["seo"],
            "accessibility_score": scores["accessibility"],
            "best_practices_score": scores["best-practices"],
            "pwa_score": scores["pwa"]
        })

        count += 1
        time.sleep(1)

    con.commit()
    con.close()

    # Push to destination
    dest = get_active_destination(uid)

    if dest and rows_for_destination:
        push_to_destination(dest, SOURCE, rows_for_destination)

    return {
        "status": "success",
        "count": count
    }