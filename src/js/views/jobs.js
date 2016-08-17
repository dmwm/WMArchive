var app = app || {};

app.JobsView = Backbone.View.extend({

  template: _.template('<%=jobs%>'),

  initialize: function() {
    this.requestJobs();
  },

  render: function() {
    this.$el.html(this.template({ jobs: this.jobs == null ? '' : this.jobs }));
  },

  requestJobs: function() {

    this.status = 'Loading jobs...';
    this.render();

    var self = this;
    $.ajax({
        url: '/wmarchive/data/?jobs=1',
        contentType: "application/json",
        type: 'GET',
        cache: false,
    }).done(function(data, msg, xhr) {
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
        self.jobs = html;
        self.render();
    });
  },

});
