// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.StatusView = Backbone.View.extend({

  template: _.template('<%=status%>'),

  initialize: function() {
    this.requestStatus();
  },

  render: function() {
    this.$el.html(this.template({ status: this.status == null ? '' : this.status }));
  },

  requestStatus: function() {

    this.status = 'Loading status...';
    this.render();

    var self = this;
    $.ajax({
        url: '/wmarchive/data/?status=1',
        contentType: "application/json",
        type: 'GET',
        cache: false,
    }).done(function(data, msg, xhr) {
        var html = '';
        var res = data.result;
        for (var i=0; i<res.length; i++) {
            var d = res[i].status;
            var sts = d.sts;
            var lts = d.lts;
            html += '<h5>Short-Term Storage</h5>\n';
            stats = sts.stats;
            html += 'Number of docs: ' + stats.count+'<br/>';
            html += 'Storage size: ' + sizeFormat(stats.storageSize) +'<br/>';
            html += 'Index size: ' + sizeFormat(stats.totalIndexSize) +'<br/>';
            for (key in stats.indexSizes) {
                html += key + ': ' + sizeFormat(stats.indexSizes[key]) + '<br/>';
            }
            if (lts != null) {
                html += '<h5>Long-Term Storage</h5>\n';
                html += 'Docs in queue: '+ lts.qsize + '<br/>\n';
                var docs = lts.pids-lts.qsize;
                html += 'Docs in progress: '+ docs + '<br/>\n';
            }
        }

        self.status = html;
        self.render();
    });
  },

});

function sizeFormat(v) {
    var o = v;
    var base = 1000. // CMS convention to use power of 10
    var xlist = ['', 'KB', 'MB', 'GB', 'TB', 'PB'];
    for(var i=0;i<xlist.length;i++) {
        if(v<base) {
            return v.toFixed(2)+xlist[i];
        }
        v /= base
    }
    return v.toFixed(2)+xlist[i];
}
