# Flask framework for web client simulation
from flask import Flask, request, render_template_string

# Environment variable handling
import os

# HTTP client (optional use)
import requests


# Identity Server base URL
IDENTITY_SERVER = os.getenv(
    "IDENTITY_SERVER",
    "http://127.0.0.1:4000"
)

# Create Flask app
app = Flask(__name__)


# -----------------------------
# Frontend UI and Tracking Script
# -----------------------------
# Embedded HTML and JavaScript
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

<!-- Displays unified user ID -->
<p>ID: <b id="uid">{{uid}}</b></p>

<h3>Profile</h3>

<input id="name" placeholder="Name">
<input id="age" placeholder="Age">
<input id="gender" placeholder="Gender">
<input id="city" placeholder="City">
<input id="country" placeholder="Country">
<input id="profession" placeholder="Profession">

<button onclick="saveProfile()">Save Profile</button>

<hr>

<h3>Upload File</h3>

<form id="uploadForm" enctype="multipart/form-data">
<input type="file" name="file">
<button type="submit">Upload</button>
</form>

<hr>

<h3>Submit Form</h3>

<input id="f1" placeholder="Feedback">
<button onclick="sendForm()">Send</button>

<hr>

<button onclick="openLogs()">View Logs</button>

</div>


<script>

const ID="{{identity}}";
const domain="{{domain}}";


/* Sends web activity and metadata */
function record(uid){

fetch(ID+"/record",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
uid:uid,
domain:domain,
meta:{
screen:screen.width+"x"+screen.height,
language:navigator.language,
timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,
page_url:location.href
}})
});

}


/* Saves user profile information */
function saveProfile(){

fetch(ID+"/profile",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
uid:uid.innerText,
name:name.value,
age:age.value,
gender:gender.value,
city:city.value,
country:country.value,
profession:profession.value
})
});

alert("Saved");

}


/* Handles file uploads */
uploadForm.onsubmit = async(e)=>{

e.preventDefault();

let form = new FormData(uploadForm);
form.append("uid",uid.innerText);

await fetch(ID+"/upload",{
method:"POST",
body:form
});

alert("Uploaded");

}


/* Sends form data */
function sendForm(){

fetch(ID+"/form/submit",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
uid:uid.innerText,
form:"feedback",
feedback:f1.value
})
});

alert("Sent");

}


/* Receives UID from iframe */
window.addEventListener("message",(e)=>{

if(e.data.type==="IDENTITY_SYNC"){
uid.innerText=e.data.uid;
record(e.data.uid);
}

});


/* Injects iframe for identity sync */
function inject(){

let f=document.createElement("iframe");
f.src=ID+"/iframe_sync";
f.style.display="none";
document.body.appendChild(f);

}


/* Opens monitoring dashboard */
function openLogs(){

window.open(ID+"/logs","_blank");

}


/* Initialize identity sync */
inject();

</script>

</body>
</html>
"""


# -----------------------------
# Main Route
# -----------------------------
# Renders client application
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

    # Start client app
    app.run(
        port=int(os.getenv("PORT","5000")),
        debug=True,
        host="0.0.0.0"
    )