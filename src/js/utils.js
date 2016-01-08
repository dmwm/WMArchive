/*
 * WMArchive utilities
 * Author: Valentin Kuznetsov
 */
function loadContent() {
    html = '<div id="header" class="header shadow"></div><div id="center"></div><div id="footer"></div>';
    $("#content").append(html);
    img = '<img src="/wmarchive/web/static/images/cms_logo.png" alt="" style="width:30px">&nbsp;';
    title = '<h3>' + img + ' CMS WMArchive data-service</h3>'
    $("#header").html('<row centered><column cols="7">' + title + '</column></row>');
    html = '<div id="menu"></div><div id="page"></div>'
    $("#center").html('<row centered><column cols="7">' + html + '</column></row>');
    html = '<blocks col="4">' +
        '<div><button type="black" upper outline onclick="ShowMenu(\'home\')">Home</button></div>' +
        '<div><button type="black" upper outline onclick="ShowMenu(\'apis\')">APIs</button></div>' +
        '<div><button type="black" upper outline onclick="ShowMenu(\'tools\')">Tools</button></div>' +
        '<div><button type="black" upper outline onclick="ShowMenu(\'mr\')">MapReduce</button></div>';
    $("#menu").html(html);
    home  = '<div id="home" class="show"></div>';
    apis  = '<div id="apis" class="hide"></div>';
    tools = '<div id="tools" class="hide"></div>';
    mr    = '<div id="mr" class="hide"></div>';
    html  = '<div>' + home + apis + mr + tools + '</div>';
    $("#page").html(html);
    var today = new Date();
    html = 'CMS collaboration: ' + today;
    $("#footer").html('<row centered><column cols="10">' + html + '</column></row>');
    $("#footer").css("text-align", "center");
	// load templates in their div containers
    $("#home").load("/wmarchive/web/static/templates/home.html");
    $("#apis").load("/wmarchive/web/static/templates/apis.html");
    $("#mr").load("/wmarchive/web/static/templates/mr.html");
    $("#tools").load("/wmarchive/web/static/templates/tools.html");
}
function ShowMenu(tag) {
    HideTag('home');
    HideTag('apis');
    HideTag('tools');
    HideTag('mr');
    ShowTag(tag);
}
function updateTag(tag, val) {
   var id = document.getElementById(tag);
   if (id) {
       id.value=val;
   }
}
function ClearTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.innerHTML="";
    }
}
function HideTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="hide";
    }
}
function ShowTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="show";
    }
}
function FlipTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide";
        } else {
            id.className="show";
        }
    }
}
function ToggleTag(tag, link_tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide";
        } else {
            id.className="show";
        }
    }
    var lid=document.getElementById(link_tag);
    if (lid) {
        if  (lid.innerHTML == "show") {
            lid.innerHTML="hide";
        } else {
            lid.innerHTML="show";
        }
    }
}
function load(url) {
    window.location.href=url;
}
function reload() {
    load(window.location.href);
}
