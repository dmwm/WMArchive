/*
 * WMArchive utilities
 * Author: Valentin Kuznetsov
 */
function loadContent() {
    html = '<div id="header" class="header shadow"></div><div id="center"></div><div id="footer"></div>';
    $("#main-content").append(html);
    img = '<img src="/wmarchive/web/static/images/cms_logo.png" alt="" style="width:30px">&nbsp;';
    title = '<h3>' + img + ' CMS WMArchive data-service</h3>'
    $("#header").html('<row centered><column cols="8">' + title + '</column></row>');
    html = '<div id="menu"></div><div id="page"></div>'
    $("#center").html('<row centered><column cols="8">' + html + '</column></row>');
    html = '<blocks col="6">' +
        '<div id="button_home"   ><button type="black" upper outline onclick="ajaxRequestStatus();ShowMenu(\'home\')">Home</button></div>' +
        '<div id="button_apis"   ><button type="black" upper outline onclick="ShowMenu(\'apis\')">APIs</button></div>' +
        '<div id="button_tools"  ><button type="black" upper outline onclick="ShowMenu(\'tools\')">Tools</button></div>' +
        '<div id="button_queries"><button type="black" upper outline onclick="ShowMenu(\'queries\')">Queries</button></div>' +
        '<div id="button_jobs"   ><button type="black" upper outline onclick="ajaxRequestJobs();ShowMenu(\'jobs\')">Jobs</button></div>' +
        '<div id="button_stats"  ><button type="black" upper outline onclick="ShowMenu(\'stats\')">Stats</button></div>';
    $("#menu").html(html);
    home    = '<div id="home" class="show"></div>';
    apis    = '<div id="apis" class="hide"></div>';
    tools   = '<div id="tools" class="hide"></div>';
    queries = '<div id="queries" class="hide"></div>';
    jobs    = '<div id="jobs" class="hide"></div>';
    stats   = '<div id="stats" class="hide"></div>';
    html    = '<div>' + home + apis + tools + queries + jobs + stats + '</div>';
    $("#page").html(html);
    var today = new Date();
    html = 'CMS collaboration: ' + today;
    $("#footer").html('<row centered><column cols="10">' + html + '</column></row>');
    $("#footer").css("text-align", "center");
	// load templates in their div containers
    $("#home").load("/wmarchive/web/static/templates/home.html");
    $("#apis").load("/wmarchive/web/static/templates/apis.html");
    $("#tools").load("/wmarchive/web/static/templates/tools.html");
    $("#queries").load("/wmarchive/web/static/templates/queries.html");
    $("#jobs").load("/wmarchive/web/static/templates/jobs.html");
    $("#stats").load("/wmarchive/web/static/templates/stats.html");
    ajaxRequestStatus();
    ShowMenu('home');
}
function ShowButton(tag) {
    ShowTag(tag);
    $('#button_'+tag).addClass('grey small-shadow');
}
function HideButton(tag) {
    HideTag(tag);
    $('#button_'+tag).removeClass('grey small-shadow');
}
function ShowMenu(tag) {
    HideButton('home');
    HideButton('apis');
    HideButton('tools');
    HideButton('queries');
    HideButton('jobs');
    HideButton('stats');
    ShowButton(tag);
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
function ajaxRequestStatus() {
    var request = $.ajax({
        url: '/wmarchive/data/?status=1',
        contentType: "application/json",
        type: 'GET',
        cache: false,
    });
    request.done(function(data, msg, xhr) {
        var html = '';
        var res = data.result;
        for(var i=0; i<res.length; i++) {
            var d = res[i].status;
            var sts = d.sts;
            var lts = d.lts;
            html += '<h5>Short-Term Storage</h5>\n';
            stats = sts.stats;
            html += 'Number of docs: ' + stats.count+'<br/>';
            html += 'Storage size:' + sizeFormat(stats.storageSize) +'<br/>';
            html += 'Index size:' + sizeFormat(stats.totalIndexSize) +'<br/>';
            for(key in stats.indexSizes) {
                html += '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'+key+': '+sizeFormat(stats.indexSizes[key])+'<br/>\n';
            }
            if (lts != null) {
                html += '<h5>Long-Term Storage</h5>\n';
                html += 'Docs in queue: '+ lts.qsize + '<br/>\n';
                var docs = lts.pids-lts.qsize;
                html += 'Docs in progress: '+ docs + '<br/>\n';
            }
        }
        $('#wmaStatus').html(html);
    });
}
function sizeFormat(v) {
    var o = v;
    var base = 1000. // CMS convention to use power of 10
    var xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB'];
    for(var i=0;i<xlist.length;i++) {
        if(v<base) {
            return o+' ('+v.toFixed(2)+xlist[i]+')';
        }
        v /= base
    }
    return o+' ('+v.toFixed(2)+xlist[i]+')';
}
function ajaxRequestJobs() {
    var request = $.ajax({
        url: '/wmarchive/data/?jobs=1',
        contentType: "application/json",
        type: 'GET',
        cache: false,
    });
    request.done(function(data, msg, xhr) {
        var html = '';
        var res = data.result;
        for(var i=0; i<res.length; i++) {
            var jobs = res[i].jobs;
            html += '<h5>Available jobs output:</h5>\n';
            for(var k=0;k<jobs.length;k++) {
                var j = jobs[k];
                html += '<a href="/wmarchive/data/'+j.wmaid+'">'+j.wmaid+'</a><br/>\n';
            }
        }
        $('#wmaJobs').html(html);
    });
}
