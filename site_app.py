# Flask framework for web client simulation
from flask import Flask, request, render_template_string

# Environment variable handling
import os


# Identity Server base URL
IDENTITY_SERVER = os.getenv(
    "IDENTITY_SERVER",
    "http://127.0.0.1:4000"
)

# Create Flask app
app = Flask(__name__)

# -----------------------------
# Frontend UI and Tracking SDK
# -----------------------------
HTML = """
<!doctype html>
<html>
<head>
<title>{{site}}</title>
<style>
body{font-family:Arial;background:#eef1f5}
.card{background:white;padding:25px;width:900px;margin:auto;margin-top:50px}
input,button{padding:6px;margin:4px;width:100%}
</style>
</head>

<body>

<div class="card">

<h2>{{site}}</h2>

<p>User ID: <b id="uid">{{uid}}</b></p>


<!-- Profile -->
<h3>Profile</h3>

<input id="email" placeholder="Email">

<input id="name" placeholder="Name">
<input id="age" placeholder="Age">
<input id="gender" placeholder="Gender">
<input id="city" placeholder="City">
<input id="country" placeholder="Country">
<input id="profession" placeholder="Profession">

<button onclick="saveProfile()">Save Profile</button>

<hr>


<!-- File Upload -->
<h3>Upload File</h3>

<form id="uploadForm" enctype="multipart/form-data">

<input type="file" name="file">

<button type="submit">Upload</button>

</form>

<hr>


<!-- Form -->
<h3>Submit Feedback</h3>

<input id="f1" placeholder="Feedback">

<button onclick="sendForm()">Send</button>

<hr>


<button onclick="openLogs()">View Logs</button>

</div>


<script>

const ID="{{identity}}";
const domain="{{domain}}";


/* -----------------------------
   Identity Utilities
----------------------------- */

function getDeviceId(){

  let d = localStorage.getItem("device_id");

  if(!d){
    d = crypto.randomUUID();
    localStorage.setItem("device_id", d);
  }

  return d;
}


function getSessionId(){

  let s = sessionStorage.getItem("session_id");

  if(!s){
    s = crypto.randomUUID();
    sessionStorage.setItem("session_id", s);
  }

  return s;
}



/* -----------------------------
   Core Event Sender
----------------------------- */

function sendEvent(type, meta={}){

fetch(ID+"/record",{

method:"POST",

headers:{
  "Content-Type":"application/json"
},

body:JSON.stringify({

uid: uid.innerText,
domain: domain,

email: email.value || null,

device_id: getDeviceId(),
session_id: getSessionId(),

event_type: type,

meta: {

screen: screen.width+"x"+screen.height,
language: navigator.language,
timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,

page_url: location.href,
referrer: document.referrer,
title: document.title,

...meta
}

})

});

}



/* -----------------------------
   Profile Save
----------------------------- */

function saveProfile(){

fetch(ID+"/profile",{

method:"POST",

headers:{
  "Content-Type":"application/json"
},

body:JSON.stringify({

uid: uid.innerText,

email: email.value,

name: name.value,
age: age.value,
gender: gender.value,
city: city.value,
country: country.value,
profession: profession.value

})

});

alert("Profile Saved");

}



/* -----------------------------
   File Upload
----------------------------- */

uploadForm.onsubmit = async(e)=>{

e.preventDefault();

let form = new FormData(uploadForm);

form.append("uid", uid.innerText);

await fetch(ID+"/upload",{
method:"POST",
body:form
});

alert("Uploaded");

}



/* -----------------------------
   Form Submit
----------------------------- */

function sendForm(){

fetch(ID+"/form/submit",{

method:"POST",

headers:{
  "Content-Type":"application/json"
},

body:JSON.stringify({

uid: uid.innerText,
form:"feedback",
feedback:f1.value

})

});

alert("Sent");

}



/* -----------------------------
   Identity Sync
----------------------------- */

window.addEventListener("message",(e)=>{

if(e.data.type==="IDENTITY_SYNC"){

uid.innerText = e.data.uid;

/* Initial Page View */
sendEvent("page_view");

}

});


function inject(){

let f = document.createElement("iframe");

f.src = ID+"/iframe_sync";

f.style.display="none";

document.body.appendChild(f);

}



/* -----------------------------
   Advanced Tracking
----------------------------- */

/* Click Tracking */
document.addEventListener("click", e=>{

sendEvent("click", {
  tag: e.target.tagName,
  text: e.target.innerText?.slice(0,50)
});

});


/* Scroll Tracking */
let maxScroll = 0;

window.addEventListener("scroll", ()=>{

let sc = Math.round(
 (window.scrollY /
 (document.body.scrollHeight-window.innerHeight))*100
);

if(sc > maxScroll){

  maxScroll = sc;

  sendEvent("scroll", {percent: sc});

}

});


/* Time On Page */
let startTime = Date.now();

window.addEventListener("beforeunload", ()=>{

let t = Math.round((Date.now()-startTime)/1000);

navigator.sendBeacon(

ID+"/record",

JSON.stringify({

uid: uid.innerText,
domain: domain,

device_id: getDeviceId(),
session_id: getSessionId(),

event_type:"time_spent",

meta:{seconds:t}

})

);

});



/* -----------------------------
   Logs
----------------------------- */

function openLogs(){

window.open(ID+"/logs","_blank");

}



/* -----------------------------
   Init
----------------------------- */

inject();

</script>

</body>
</html>
"""


# -----------------------------
# Main Route
# -----------------------------
@app.route("/")
def index():

    return render_template_string(

        HTML,

        site=os.getenv("SITE_NAME", "Site"),

        domain=os.getenv("DOMAIN_NAME", "local"),

        identity=IDENTITY_SERVER,

        uid=request.cookies.get("uid","")

    )


# -----------------------------
# Application Entry Point
# -----------------------------
if __name__ == "__main__":

    app.run(

        port=int(os.getenv("PORT","5000")),

        debug=True,

        host="0.0.0.0"

    )