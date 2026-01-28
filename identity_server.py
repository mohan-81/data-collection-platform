from flask import Flask, request, redirect, make_response, jsonify, render_template_string
import sqlite3, uuid, datetime, os, json
from flask_cors import CORS
import zoneinfo
from user_agents import parse
import pandas as pd
from tika import parser
import xmltodict
import requests

# Timezone
IST = zoneinfo.ZoneInfo("Asia/Kolkata")

app = Flask(__name__)
CORS(app, supports_credentials=True)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

DB = "identity.db"


# -----------------------------
# Database Initialization
# -----------------------------
def init_db():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Visits Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT,
            domain TEXT,
            browser TEXT,
            os TEXT,
            device TEXT,
            ip TEXT,
            screen TEXT,
            language TEXT,
            timezone TEXT,
            referrer TEXT,
            page_url TEXT,
            user_agent TEXT,
            name TEXT,
            age INTEGER,
            gender TEXT,
            city TEXT,
            country TEXT,
            profession TEXT,
            ts TEXT
        )
    """)

    # File Data Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS file_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT,
            filename TEXT,
            filetype TEXT,
            content TEXT,
            ts TEXT
        )
    """)

    # API Data Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            endpoint TEXT,
            payload TEXT,
            ts TEXT
        )
    """)

    # Form Data Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS form_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uid TEXT,
            form_name TEXT,
            data TEXT,
            ts TEXT
        )
    """)

    conn.commit()
    conn.close()


# -----------------------------
# Identity Sync
# -----------------------------
@app.route("/sync")
def sync():
    return_url = request.args.get("return_url")

    uid = request.cookies.get("uid")
    if not uid:
        uid = str(uuid.uuid4())

    resp = make_response(redirect(f"{return_url}?uid={uid}"))
    resp.set_cookie("uid", uid, max_age=60 * 60 * 24 * 30)

    return resp


# -----------------------------
# Iframe Sync
# -----------------------------
IFRAME = """
<script>
window.parent.postMessage({type:"IDENTITY_SYNC", uid:"{{uid}}"}, "*");
</script>
"""


@app.route("/iframe_sync")
def iframe_sync():
    uid = request.cookies.get("uid")

    if not uid:
        uid = str(uuid.uuid4())

    resp = make_response(render_template_string(IFRAME, uid=uid))
    resp.set_cookie("uid", uid, max_age=60 * 60 * 24 * 30)

    return resp


# -----------------------------
# Web Visit Collection
# -----------------------------
@app.route("/record", methods=["POST"])
def record():

    data = request.get_json() or {}

    uid = data.get("uid")
    domain = data.get("domain")
    meta = data.get("meta", {})

    if not uid or not domain:
        return jsonify({"error": "missing"}), 400

    ua_string = request.headers.get("User-Agent")
    ip = request.remote_addr

    ua = parse(ua_string)

    browser = ua.browser.family
    os_name = ua.os.family

    if ua.is_mobile:
        device = "Mobile"
    elif ua.is_tablet:
        device = "Tablet"
    else:
        device = "Desktop"

    ts = datetime.datetime.now(IST).isoformat()

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO visits (
            uid, domain, browser, os, device, ip,
            screen, language, timezone,
            referrer, page_url, user_agent, ts
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        uid, domain, browser, os_name, device, ip,
        meta.get("screen"),
        meta.get("language"),
        meta.get("timezone"),
        meta.get("referrer"),
        meta.get("page_url"),
        ua_string,
        ts
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})


# -----------------------------
# Save Profile
# -----------------------------
@app.route("/profile", methods=["POST"])
def save_profile():

    data = request.get_json() or {}
    uid = data.get("uid")

    if not uid:
        return jsonify({"error": "uid missing"}), 400

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        UPDATE visits
        SET name=?, age=?, gender=?, city=?, country=?, profession=?
        WHERE uid=?
    """, (
        data.get("name"),
        data.get("age"),
        data.get("gender"),
        data.get("city"),
        data.get("country"),
        data.get("profession"),
        uid
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "saved"})


# -----------------------------
# File Upload Collector
# -----------------------------
@app.route("/upload", methods=["POST"])
def upload_file():

    uid = request.form.get("uid")

    file = request.files.get("file")

    if not file:
        return "No file", 400

    filename = file.filename
    path = os.path.join(UPLOAD_FOLDER, filename)

    file.save(path)

    content = ""

    # CSV / Excel
    if filename.endswith(".csv") or filename.endswith(".xlsx"):

        df = pd.read_csv(path) if filename.endswith(".csv") else pd.read_excel(path)
        content = df.to_json()

    # JSON
    elif filename.endswith(".json"):

        with open(path) as f:
            content = json.dumps(json.load(f))

    # XML
    elif filename.endswith(".xml"):

        with open(path) as f:
            content = json.dumps(xmltodict.parse(f.read()))

    # PDF / DOC
    else:

        parsed = parser.from_file(path)
        content = parsed.get("content", "")

    ts = datetime.datetime.now(IST).isoformat()

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO file_data
        (uid, filename, filetype, content, ts)
        VALUES (?,?,?,?,?)
    """, (
        uid,
        filename,
        filename.split(".")[-1],
        content,
        ts
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "uploaded"})


# -----------------------------
# API Collector
# -----------------------------
@app.route("/api/collect", methods=["POST"])
def api_collect():

    data = request.get_json()

    source = data.get("source")
    endpoint = data.get("endpoint")
    payload = json.dumps(data.get("data"))

    ts = datetime.datetime.now(IST).isoformat()

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO api_data
        (source, endpoint, payload, ts)
        VALUES (?,?,?,?)
    """, (
        source,
        endpoint,
        payload,
        ts
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "api stored"})


# -----------------------------
# Form Collector
# -----------------------------
@app.route("/form/submit", methods=["POST"])
def submit_form():

    data = request.get_json()

    uid = data.get("uid")
    form_name = data.get("form")

    ts = datetime.datetime.now(IST).isoformat()

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO form_data
        (uid, form_name, data, ts)
        VALUES (?,?,?,?)
    """, (
        uid,
        form_name,
        json.dumps(data),
        ts
    ))

    conn.commit()
    conn.close()

    return jsonify({"status": "form saved"})


# -----------------------------
# Logs UI
# -----------------------------
@app.route("/logs")
def logs():

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    cur.execute("SELECT * FROM visits ORDER BY id DESC LIMIT 50")
    visits = cur.fetchall()

    cur.execute("SELECT * FROM file_data ORDER BY id DESC LIMIT 20")
    files = cur.fetchall()

    cur.execute("SELECT * FROM api_data ORDER BY id DESC LIMIT 20")
    apis = cur.fetchall()

    cur.execute("SELECT * FROM form_data ORDER BY id DESC LIMIT 20")
    forms = cur.fetchall()

    conn.close()

    return render_template_string("""
    <h2>Visits</h2>{{v}}
    <h2>Files</h2>{{f}}
    <h2>APIs</h2>{{a}}
    <h2>Forms</h2>{{fo}}
    """,
    v=visits,
    f=files,
    a=apis,
    fo=forms)


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    init_db()

    app.run(
        port=4000,
        debug=True,
        host="0.0.0.0"
    )