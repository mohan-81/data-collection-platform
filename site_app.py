from flask import Flask, render_template_string, request, redirect, make_response
import os


IDENTITY_SERVER = os.getenv(
    "IDENTITY_SERVER",
    "http://127.0.0.1:4000"
)

app = Flask(__name__)


HTML = """
<!doctype html>
<html>
<head>

<title>{{site}}</title>

<style>

body{
font-family:Inter,Arial;
background:linear-gradient(to right,#0f172a,#020617);
color:white;
margin:0;
}

.nav{
background:#020617;
padding:15px 40px;
display:flex;
justify-content:space-between;
align-items:center;
border-bottom:1px solid #1e293b;
}

.nav h2{
color:#22d3ee;
margin:0;
}

.nav a{
color:#cbd5f5;
text-decoration:none;
margin-left:20px;
font-size:14px;
}

.nav a:hover{
color:#22d3ee;
}

.container{
max-width:1000px;
margin:auto;
padding:40px 20px;
}

.card{
background:#020617;
padding:30px;
border-radius:12px;
border:1px solid #1e293b;
box-shadow:0 0 25px rgba(34,211,238,0.05);
}

input,button{
padding:10px;
margin:8px 0;
width:100%;
border-radius:6px;
border:1px solid #1e293b;
background:#020617;
color:white;
}

input:focus{
outline:none;
border-color:#22d3ee;
}

button{
background:#22d3ee;
color:black;
font-weight:bold;
cursor:pointer;
border:none;
}

button:hover{
background:#67e8f9;
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(250px,1fr));
gap:20px;
margin-top:20px;
}

.box{
padding:20px;
background:#020617;
border:1px solid #1e293b;
border-radius:10px;
}

.box h3{
color:#22d3ee;
margin-top:0;
}

.footer{
text-align:center;
padding:20px;
color:#94a3b8;
font-size:13px;
border-top:1px solid #1e293b;
margin-top:60px;
}

</style>

</head>


<body>


<div class="nav">
<h2>Segmento Demo</h2>

<div>
<a href="http://127.0.0.1:3000">Platform</a>
<a href="http://127.0.0.1:3000/tracking">Tracking</a>
<a href="http://127.0.0.1:3000/dashboard">Dashboard</a>
{% if email %}
<a href="/logout">Logout</a>
{% endif %}
</div>
</div>


<div class="container">


{% if not email %}

<!-- LOGIN -->

<div class="card">

<h1>Welcome to Segmento Demo Site</h1>

<p style="color:#94a3b8">
This website simulates a real customer using Segmento Collector.
</p>

<hr style="border-color:#1e293b">

<h3>Login / Signup</h3>

<form method="POST" action="/login">

<input name="email" placeholder="Enter your email" required>

<button type="submit">Continue</button>

</form>

</div>


{% else %}

<!-- DASHBOARD -->

<div class="card">

<h1>Dashboard</h1>

<p style="color:#94a3b8">
Logged in as <b>{{email}}</b>
</p>

<p>
This website is actively tracked using Segmento JS SDK.
</p>

</div>


<div class="grid">


<!-- PROFILE -->

<div class="box">

<h3>User Profile</h3>

<p>Email: {{email}}</p>
<p>Status: Active</p>
<p>Plan: Demo</p>

</div>


<!-- ACTIONS -->

<div class="box">

<h3>Test Actions</h3>

<button>Buy Now</button>

<button>Add to Cart</button>

<button>Subscribe</button>

</div>


<!-- ACTIVITY -->

<div class="box">

<h3>Activity Simulator</h3>

<p>Scroll, click, and navigate this page.</p>

<p>All actions are tracked automatically.</p>

<button onclick="fakeAction()">Simulate Event</button>

</div>


</div>


<div class="box" style="margin-top:30px;text-align:center">

<h3>Collector Status</h3>

<p style="color:#22d3ee;font-weight:bold">
Live Tracking Enabled
</p>

<p style="color:#94a3b8">
Events are being sent to Identity Server
</p>

</div>


{% endif %}

</div>


<div class="footer">
© 2026 Segmento Data Technologies — Demo Client
</div>



{% if email %}

<!-- Segmento SDK -->
<script src="http://127.0.0.1:4000/static/sdk/segmento.js"></script>

<script>

// Save email for dashboard access
localStorage.setItem("segmento_email","{{email}}");


// Init SDK
Segmento.init("http://127.0.0.1:4000");


// Sync email after identity created
window.addEventListener("message",(e)=>{

if(e.data.type==="IDENTITY_SYNC"){

fetch("http://127.0.0.1:4000/profile",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
uid:e.data.uid,
email:"{{email}}"
})
});

}

});


function fakeAction(){
alert("Test event triggered");
}

</script>

{% endif %}


</body>
</html>
"""


# ---------------- ROUTES ----------------


@app.route("/", methods=["GET"])
def index():

    email = request.cookies.get("email")

    return render_template_string(
        HTML,
        site="Segmento Demo Site",
        email=email
    )


@app.route("/login", methods=["POST"])
def login():

    email = request.form.get("email")

    resp = make_response(redirect("/"))

    resp.set_cookie("email", email, max_age=30*24*3600)

    return resp


@app.route("/logout")
def logout():

    resp = make_response(redirect("/"))

    resp.delete_cookie("email")

    return resp



# ---------------- RUN ----------------

if __name__ == "__main__":

    app.run(
        port=5000,
        debug=True,
        host="0.0.0.0"
    )