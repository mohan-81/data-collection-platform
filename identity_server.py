# Core web framework for building APIs
from flask import Flask, request, redirect, make_response, jsonify, render_template_string

# Standard libraries
import sqlite3
import uuid
import datetime
import os
import json

# Enable cross-domain communication
from flask_cors import CORS

# Timezone handling
import zoneinfo

# Used to detect browser, OS, device type
from user_agents import parse

# Used for CSV/Excel processing
import pandas as pd

# Used for PDF/DOC parsing
from tika import parser

# Used for XML parsing
import xmltodict

# Used for external API calls (if needed)
import requests


# -----------------------------
# Configuration
# -----------------------------

# Set timezone to IST
IST = zoneinfo.ZoneInfo("Asia/Kolkata")

# Create Flask application
app = Flask(__name__)

# Enable CORS for cross-domain tracking
CORS(app, supports_credentials=True)

# Folder for uploaded files
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# Central database file
DB = "identity.db"


# -----------------------------
# Database Initialization
# -----------------------------
# Creates required tables for unified storage
def init_db():

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    # Table for web visits and identity metadata
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

    # Table for uploaded file data
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

    # Table for external API data
    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            endpoint TEXT,
            payload TEXT,
            ts TEXT
        )
    """)

    # Table for form submissions
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
# Identity Synchronization
# -----------------------------
# Generates and syncs user ID across domains
@app.route("/sync")
def sync():

    return_url = request.args.get("return_url")

    # Check if UID already exists
    uid = request.cookies.get("uid")

    # Generate new UID if not present
    if not uid:
        uid = str(uuid.uuid4())

    # Redirect back with UID
    resp = make_response(redirect(f"{return_url}?uid={uid}"))

    # Store UID in cookie
    resp.set_cookie("uid", uid, max_age=60 * 60 * 24 * 30)

    return resp


# -----------------------------
# Iframe Sync (Cross-Domain)
# -----------------------------

# Script injected into iframe
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
# Web Event Collection
# -----------------------------
# Collects client-side activity data
@app.route("/record", methods=["POST"])
def record():

    data = request.get_json() or {}

    uid = data.get("uid")
    domain = data.get("domain")
    meta = data.get("meta", {})

    # Validate required fields
    if not uid or not domain:
        return jsonify({"error": "missing"}), 400

    # Capture user agent and IP
    ua_string = request.headers.get("User-Agent")
    ip = request.remote_addr

    ua = parse(ua_string)

    browser = ua.browser.family
    os_name = ua.os.family

    # Detect device type
    if ua.is_mobile:
        device = "Mobile"
    elif ua.is_tablet:
        device = "Tablet"
    else:
        device = "Desktop"

    ts = datetime.datetime.now(IST).isoformat()

    # Store visit record
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
# Profile Storage
# -----------------------------
# Saves user profile information
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
# File Ingestion
# -----------------------------
# Handles file upload and parsing
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

    # CSV / Excel processing
    if filename.endswith(".csv") or filename.endswith(".xlsx"):

        df = pd.read_csv(path) if filename.endswith(".csv") else pd.read_excel(path)
        content = df.to_json()

    # JSON file
    elif filename.endswith(".json"):

        with open(path) as f:
            content = json.dumps(json.load(f))

    # XML file
    elif filename.endswith(".xml"):

        with open(path) as f:
            content = json.dumps(xmltodict.parse(f.read()))

    # PDF / DOC parsing
    else:

        parsed = parser.from_file(path)
        content = parsed.get("content", "")

    ts = datetime.datetime.now(IST).isoformat()

    # Store file data
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
# API Ingestion
# -----------------------------
# Central endpoint for external connectors
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
# Form Ingestion
# -----------------------------
# Collects structured form submissions
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
# Monitoring UI
# -----------------------------
# Displays recent collected data
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
# Application Entry Point
# -----------------------------
if __name__ == "__main__":

    # Initialize database
    init_db()

    # Start ingestion server
    app.run(
        port=4000,
        debug=True,
        host="0.0.0.0"
    )