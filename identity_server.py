from flask import Flask,request,redirect,make_response,jsonify,render_template_string
import sqlite3,uuid,datetime,os,json
from flask_cors import CORS
import zoneinfo
from user_agents import parse
import pandas as pd
from tika import parser
import xmltodict
import requests

# ---------- Config ----------
IST=zoneinfo.ZoneInfo("Asia/Kolkata")
app=Flask(__name__)
CORS(app,supports_credentials=True)
UPLOAD_FOLDER="uploads"
os.makedirs(UPLOAD_FOLDER,exist_ok=True)
DB="identity.db"

# ---------- DB Init ----------
def init_db():

    con=sqlite3.connect(DB)
    cur=con.cursor()

    # Visits (device + user info)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS visits(
    id INTEGER PRIMARY KEY,
    uid TEXT,domain TEXT,browser TEXT,os TEXT,device TEXT,ip TEXT,
    screen TEXT,language TEXT,timezone TEXT,
    referrer TEXT,page_url TEXT,user_agent TEXT,
    name TEXT,age INTEGER,gender TEXT,city TEXT,country TEXT,profession TEXT,
    ts TEXT)
    """)

    # Identity stitching
    cur.execute("""
    CREATE TABLE IF NOT EXISTS identity_map(
    id INTEGER PRIMARY KEY,
    uid TEXT,email TEXT,device_id TEXT,
    session_id TEXT,external_id TEXT,created_at TEXT)
    """)

    # Raw web events
    cur.execute("""
    CREATE TABLE IF NOT EXISTS web_events(
    id INTEGER PRIMARY KEY,
    uid TEXT,domain TEXT,event TEXT,
    device_id TEXT,session_id TEXT,
    meta TEXT,ts TEXT)
    """)

    # Files
    cur.execute("""
    CREATE TABLE IF NOT EXISTS file_data(
    id INTEGER PRIMARY KEY,
    uid TEXT,filename TEXT,filetype TEXT,content TEXT,ts TEXT)
    """)

    # APIs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS api_data(
    id INTEGER PRIMARY KEY,
    source TEXT,endpoint TEXT,payload TEXT,ts TEXT)
    """)

    # Forms
    cur.execute("""
    CREATE TABLE IF NOT EXISTS form_data(
    id INTEGER PRIMARY KEY,
    uid TEXT,form_name TEXT,data TEXT,ts TEXT)
    """)

    con.commit();con.close()

init_db()

# ---------- Identity Sync ----------
@app.route("/sync")
def sync():

    r=request.args.get("return_url")
    uid=request.cookies.get("uid") or str(uuid.uuid4())

    resp=make_response(redirect(f"{r}?uid={uid}"))
    resp.set_cookie("uid",uid,max_age=30*24*3600)

    return resp

IFRAME="""<script>
window.parent.postMessage({type:"IDENTITY_SYNC",uid:"{{uid}}"},"*");
</script>"""

@app.route("/iframe_sync")
def iframe_sync():

    uid=request.cookies.get("uid") or str(uuid.uuid4())

    resp=make_response(render_template_string(IFRAME,uid=uid))
    resp.set_cookie("uid",uid,max_age=30*24*3600)

    return resp

# ---------- Web Collector ----------
@app.route("/record",methods=["POST"])
def record():

    d=request.get_json() or {}

    uid=d.get("uid");domain=d.get("domain")
    email=d.get("email")
    did=d.get("device_id");sid=d.get("session_id")
    event=d.get("event_type","page")
    meta=d.get("meta",{})

    if not uid or not domain:
        return jsonify({"error":"missing"}),400

    ua=request.headers.get("User-Agent")
    ip=request.remote_addr
    p=parse(ua)

    browser=p.browser.family
    os_name=p.os.family
    device="Mobile" if p.is_mobile else "Tablet" if p.is_tablet else "Desktop"

    ts=datetime.datetime.now(IST).isoformat()

    con=sqlite3.connect(DB);cur=con.cursor()

    # Visit info
    cur.execute("""
    INSERT INTO visits
    (uid,domain,browser,os,device,ip,
    screen,language,timezone,
    referrer,page_url,user_agent,ts)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
    """,(uid,domain,browser,os_name,device,ip,
    meta.get("screen"),meta.get("language"),meta.get("timezone"),
    meta.get("referrer"),meta.get("page_url"),ua,ts))

    # Identity map
    cur.execute("""
    INSERT INTO identity_map
    (uid,email,device_id,session_id,external_id,created_at)
    VALUES(?,?,?,?,?,?)
    """,(uid,email,did,sid,None,ts))

    # Raw events
    cur.execute("""
    INSERT INTO web_events
    (uid,domain,event,device_id,session_id,meta,ts)
    VALUES(?,?,?,?,?,?,?)
    """,(uid,domain,event,did,sid,json.dumps(meta),ts))

    con.commit();con.close()

    return jsonify({"status":"stored"})

# ---------- Profile ----------
@app.route("/profile",methods=["POST"])
def profile():

    d=request.get_json() or {}

    uid=d.get("uid");email=d.get("email")

    if not uid:
        return jsonify({"error":"uid"}),400

    con=sqlite3.connect(DB);cur=con.cursor()

    cur.execute("""
    UPDATE visits SET
    name=?,age=?,gender=?,city=?,country=?,profession=?
    WHERE uid=?
    """,(d.get("name"),d.get("age"),d.get("gender"),
         d.get("city"),d.get("country"),d.get("profession"),uid))

    if email:
        cur.execute("UPDATE identity_map SET email=? WHERE uid=?",(email,uid))

    con.commit();con.close()

    return jsonify({"status":"saved"})

# ---------- File Upload ----------
@app.route("/upload",methods=["POST"])
def upload():

    uid=request.form.get("uid")
    f=request.files.get("file")

    if not f:return "No file",400

    path=os.path.join(UPLOAD_FOLDER,f.filename)
    f.save(path)

    c=""

    if f.filename.endswith((".csv",".xlsx")):
        df=pd.read_csv(path) if f.filename.endswith(".csv") else pd.read_excel(path)
        c=df.to_json()

    elif f.filename.endswith(".json"):
        c=json.dumps(json.load(open(path)))

    elif f.filename.endswith(".xml"):
        c=json.dumps(xmltodict.parse(open(path).read()))

    else:
        c=parser.from_file(path).get("content","")

    ts=datetime.datetime.now(IST).isoformat()

    con=sqlite3.connect(DB);cur=con.cursor()

    cur.execute("""
    INSERT INTO file_data
    VALUES(NULL,?,?,?,?,?)
    """,(uid,f.filename,f.filename.split(".")[-1],c,ts))

    con.commit();con.close()

    return jsonify({"status":"uploaded"})

# ---------- API ----------
@app.route("/api/collect",methods=["POST"])
def api():

    d=request.get_json()
    ts=datetime.datetime.now(IST).isoformat()

    con=sqlite3.connect(DB);cur=con.cursor()

    cur.execute("""
    INSERT INTO api_data VALUES(NULL,?,?,?,?)
    """,(d.get("source"),d.get("endpoint"),
         json.dumps(d.get("data")),ts))

    con.commit();con.close()

    return jsonify({"status":"ok"})

# ---------- Form ----------
@app.route("/form/submit",methods=["POST"])
def form():

    d=request.get_json()
    ts=datetime.datetime.now(IST).isoformat()

    con=sqlite3.connect(DB);cur=con.cursor()

    cur.execute("""
    INSERT INTO form_data VALUES(NULL,?,?,?,?)
    """,(d.get("uid"),d.get("form"),json.dumps(d),ts))

    con.commit();con.close()

    return jsonify({"status":"saved"})

# ---------- Logs ----------
@app.route("/logs")
def logs():

    con=sqlite3.connect(DB);cur=con.cursor()

    cur.execute("SELECT * FROM visits ORDER BY id DESC LIMIT 20")
    v=cur.fetchall()

    cur.execute("SELECT * FROM identity_map ORDER BY id DESC LIMIT 20")
    i=cur.fetchall()

    cur.execute("SELECT * FROM web_events ORDER BY id DESC LIMIT 20")
    e=cur.fetchall()

    cur.execute("SELECT * FROM api_data ORDER BY id DESC LIMIT 10")
    a=cur.fetchall()

    cur.execute("SELECT * FROM file_data ORDER BY id DESC LIMIT 10")
    f=cur.fetchall()

    cur.execute("SELECT * FROM form_data ORDER BY id DESC LIMIT 10")
    fm=cur.fetchall()

    con.close()

    html="""
    <h3>Visits</h3><pre>{{v}}</pre>
    <h3>Identity</h3><pre>{{i}}</pre>
    <h3>Web Events</h3><pre>{{e}}</pre>
    <h3>API</h3><pre>{{a}}</pre>
    <h3>Files</h3><pre>{{f}}</pre>
    <h3>Forms</h3><pre>{{fm}}</pre>
    """

    return render_template_string(html,v=v,i=i,e=e,a=a,f=f,fm=fm)

# ---------- Run ----------
if __name__=="__main__":
    app.run(port=4000,debug=True,host="0.0.0.0")