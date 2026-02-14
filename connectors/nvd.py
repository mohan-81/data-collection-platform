import requests,sqlite3,time,os,json
from datetime import datetime
from dotenv import load_dotenv

def utc_now():
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.000Z")

load_dotenv()

DB="identity.db"

BASE="https://services.nvd.nist.gov/rest/json/cves/2.0"

API_KEY=os.getenv("NVD_API_KEY")


KEYWORDS=[
    "python",
    "linux",
    "openssl",
    "tensorflow",
    "django"
]

HEADERS={
    "User-Agent":"SegmentoCollector/1.0"
}

if API_KEY:
    HEADERS["X-Api-Key"]=API_KEY


# ---------------- DB ----------------

def db():
    con=sqlite3.connect(DB,timeout=90,check_same_thread=False,isolation_level=None)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    return con


# ---------------- HTTP ----------------

def safe_get(params):

    try:
        r=requests.get(BASE,headers=HEADERS,params=params,timeout=30)

        print("NVD URL:",r.url)
        print("NVD STATUS:",r.status_code)

        if r.status_code==200:
            return r.json()

        print("NVD BODY:",r.text[:300])

        if r.status_code==429:
            time.sleep(120)

    except Exception as e:
        print("NVD ERROR:",e)
        time.sleep(5)

    return None


# ---------------- State ----------------

def get_last_published(uid):
    con=db()
    cur=con.cursor()
    cur.execute("SELECT last_published FROM nvd_state WHERE uid=?",(uid,))
    row=cur.fetchone()
    con.close()

    if row and row[0]:
        return row[0]

    return "1999-01-01T00:00:00.000Z"


def save_last_published(uid,ts):
    con=db()
    cur=con.cursor()

    cur.execute("""
    INSERT OR REPLACE INTO nvd_state(uid,last_published)
    VALUES(?,?)
    """,(uid,ts))

    con.close()

# ---------------- Inserts ----------------

def insert_cves(uid,rows):
    con=db()
    cur=con.cursor()
    now=datetime.utcnow().isoformat()
    data=[]

    for v in rows:

        cve=v.get("cve",{})

        cve_id=cve.get("id")

        src=cve.get("sourceIdentifier")

        published=cve.get("published")

        modified=cve.get("lastModified")

        status=cve.get("vulnStatus")


        desc=""

        dlist=cve.get("descriptions",[])

        if dlist:
            desc=dlist[0].get("value")


        severity=None
        score=None


        metrics=cve.get("metrics",{})

        if "cvssMetricV31" in metrics:
            m=metrics["cvssMetricV31"][0]
            cvss=m.get("cvssData",{})
            severity=cvss.get("baseSeverity")
            score=cvss.get("baseScore")

        elif "cvssMetricV30" in metrics:
            m=metrics["cvssMetricV30"][0]
            cvss=m.get("cvssData",{})
            severity=cvss.get("baseSeverity")
            score=cvss.get("baseScore")


        ref_url=None

        refs=cve.get("references",[])

        if refs:
            ref_url=refs[0].get("url")


        data.append((
            uid,
            cve_id,
            src,
            published,
            modified,
            status,
            desc,
            severity,
            score,
            ref_url,
            json.dumps(v,ensure_ascii=False),
            now
        ))


    cur.executemany("""
    INSERT OR IGNORE INTO nvd_cves
    (uid,cve_id,source_identifier,published,last_modified,
     vuln_status,description,severity,cvss_score,
     reference_url,raw_json,fetched_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
    """,data)

    con.close()


# ---------------- Fetch ----------------

def fetch_keyword(kw,page=0):

    params={
        "keywordSearch":kw,
        "resultsPerPage":200,
        "startIndex":page*200
    }

    return safe_get(params)

# ---------------- Main Sync ----------------

def sync_nvd(uid, sync_type="incremental"):

    con = db()
    cur = con.cursor()

    # Load config
    cur.execute("""
        SELECT api_key
        FROM connector_configs
        WHERE uid=? AND connector='nvd'
        ORDER BY id DESC
        LIMIT 1
    """, (uid,))
    row = cur.fetchone()

    api_key = row[0] if row else None
    
    row = cur.fetchone()

    if not row:
        return {"error": "No NVD config"}

    config = json.loads(row[0])
    keywords = config.get("keywords", [])
    api_key = config.get("api_key")

    headers = HEADERS.copy()
    if api_key:
        headers["X-Api-Key"] = api_key

    # Historical resets state
    if sync_type == "historical":
        last_published = "1999-01-01T00:00:00.000Z"
    else:
        last_published = get_last_published(uid)

    all_rows = []
    newest_ts = last_published

    for kw in keywords:

        params = {
            "keywordSearch": kw,
            "resultsPerPage": 200,
            "lastModStartDate": last_published
        }

        r = requests.get(BASE, headers=headers, params=params, timeout=30)

        if r.status_code != 200:
            continue

        data = r.json()
        vulns = data.get("vulnerabilities", [])

        for v in vulns:
            cve = v.get("cve", {})
            pub = cve.get("published")

            if pub and pub > newest_ts:
                newest_ts = pub

            all_rows.append(v)

    if all_rows:
        insert_cves(uid, all_rows)
        save_last_published(uid, newest_ts)

    return {
        "cves": len(all_rows),
        "sync_type": sync_type
    }
