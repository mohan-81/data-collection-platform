import requests,sqlite3,time,os,json,xml.etree.ElementTree as ET
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB="identity.db"

BASE="https://api.openstreetmap.org/api/0.6"

HEADERS={
    "User-Agent":"SegmentoCollector/1.0 (contact@example.com)"
}


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(url,params=None):
    try:
        r=requests.get(url,headers=HEADERS,params=params,timeout=25)
        if r.status_code==200:
            return r.text if "xml" in r.headers.get("Content-Type","") else r.json()
        if r.status_code==429:
            time.sleep(120)
    except Exception:
        time.sleep(5)
    return None


# ---------------- State ----------------

def get_state(uid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    SELECT last_changeset_id,last_note_id
    FROM osm_state WHERE uid=?
    """,(uid,))
    row=cur.fetchone()
    con.close()

    if row:
        return row[0] or 0,row[1] or 0

    return 0,0


def save_state(uid,cid,nid):
    con=db()
    cur=con.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO osm_state
    (uid,last_changeset_id,last_note_id)
    VALUES(?,?,?)
    """,(uid,cid,nid))
    con.close()


# ---------------- Parsers ----------------

def parse_changesets(xml_text):

    root=ET.fromstring(xml_text)
    rows=[]

    for cs in root.findall("changeset"):

        rows.append({
            "id":int(cs.get("id",0)),
            "user":cs.get("user"),
            "uid":cs.get("uid"),
            "created":cs.get("created_at"),
            "closed":cs.get("closed_at"),
            "min_lat":cs.get("min_lat"),
            "min_lon":cs.get("min_lon"),
            "max_lat":cs.get("max_lat"),
            "max_lon":cs.get("max_lon"),
            "raw":ET.tostring(cs,encoding="unicode")
        })

    return rows


def parse_notes(data):

    rows=[]

    for f in data.get("features",[]):

        p=f.get("properties",{})
        g=f.get("geometry",{}) or {}
        coords=g.get("coordinates",[None,None])

        rows.append({
            "id":p.get("id"),
            "status":p.get("status"),
            "lat":coords[1],
            "lon":coords[0],
            "created":p.get("date_created"),
            "closed":p.get("date_closed"),
            "comments":len(p.get("comments",[])),
            "raw":f
        })

    return rows


# ---------------- Inserts ----------------

def insert_changesets(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]

    for r in rows:
        data.append((
            uid,
            r["id"],
            r["user"],
            r["uid"],
            r["created"],
            r["closed"],
            r["min_lat"],
            r["min_lon"],
            r["max_lat"],
            r["max_lon"],
            r["raw"],
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO osm_changesets
    (uid,changeset_id,user,uid_osm,created_at,closed_at,
     min_lat,min_lon,max_lat,max_lon,raw_xml,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


def insert_notes(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]

    for r in rows:
        data.append((
            uid,
            r["id"],
            r["status"],
            r["lat"],
            r["lon"],
            r["created"],
            r["closed"],
            r["comments"],
            json.dumps(r["raw"],ensure_ascii=False),
            now
        ))

    cur.executemany("""
    INSERT OR IGNORE INTO osm_notes
    (uid,note_id,status,lat,lon,created_at,closed_at,
     comments,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


# ---------------- Main Sync ----------------

def sync_openstreetmap(uid,limit=50):

    last_cs,last_note=get_state(uid)


    # -------- Changesets --------

    xml=safe_get(f"{BASE}/changesets",{
        "closed":"true",
        "limit":limit
    })

    new_cs=[]

    if xml:
        rows=parse_changesets(xml)

        for r in rows:
            if r["id"]>last_cs:
                new_cs.append(r)

        if new_cs:
            insert_changesets(uid,new_cs)
            last_cs=max(r["id"] for r in new_cs)


    # -------- Notes --------

    notes=safe_get(f"{BASE}/notes.json",{
        "limit":100,
        "closed":-1
    })

    new_notes=[]

    if notes:
        rows=parse_notes(notes)

        for r in rows:
            if r["id"]>last_note:
                new_notes.append(r)

        if new_notes:
            insert_notes(uid,new_notes)
            last_note=max(r["id"] for r in new_notes)


    save_state(uid,last_cs,last_note)


    return {
        "status":"ok",
        "new_changesets":len(new_cs),
        "new_notes":len(new_notes)
    }