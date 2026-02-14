(function(){

const Segmento={};
let PROJECT_KEY="";
let SERVER="";
let UID="";
let DOMAIN=location.hostname;

function uuid(){
return crypto.randomUUID();
}

function getDevice(){
let d=localStorage.getItem("segmento_device");
if(!d){
d=uuid();
localStorage.setItem("segmento_device",d);
}
return d;
}

function getSession(){
let s=sessionStorage.getItem("segmento_session");
if(!s){
s=uuid();
sessionStorage.setItem("segmento_session",s);
}
return s;
}

function send(type,meta={}){

if(!UID)return;

fetch(SERVER+"/record",{
method:"POST",
headers:{"Content-Type":"application/json"},
body:JSON.stringify({
project_key: PROJECT_KEY,
uid:UID,
domain:DOMAIN,
device_id:getDevice(),
session_id:getSession(),
event_type:type,
meta:{
url:location.href,
title:document.title,
referrer:document.referrer,
screen:screen.width+"x"+screen.height,
language:navigator.language,
...meta
}
})
});

}

function sync(){

const iframe=document.createElement("iframe");
iframe.src=SERVER+"/iframe_sync";
iframe.style.display="none";
document.body.appendChild(iframe);

}

window.addEventListener("message",(e)=>{

if(e.data.type==="IDENTITY_SYNC"){
UID=e.data.uid;
send("page_view");
}

});

function clicks(){

document.addEventListener("click",(e)=>{

send("click",{
tag:e.target.tagName,
text:e.target.innerText?.slice(0,40)
});

});

}

function scroll(){

let max=0;

window.addEventListener("scroll",()=>{

let sc=Math.round(
(window.scrollY/
(document.body.scrollHeight-window.innerHeight))*100
);

if(sc>max){
max=sc;
send("scroll",{percent:sc});
}

});

}

function time(){

let start=Date.now();

window.addEventListener("beforeunload",()=>{

let t=Math.round((Date.now()-start)/1000);

navigator.sendBeacon(
SERVER+"/record",
JSON.stringify({
uid:UID,
domain:DOMAIN,
device_id:getDevice(),
session_id:getSession(),
event_type:"time_spent",
meta:{seconds:t}
})
);

});

}

Segmento.init=function(server,key){
SERVER=server;
PROJECT_KEY=key;

SERVER=server;

sync();
clicks();
scroll();
time();

};

window.Segmento=Segmento;

})();