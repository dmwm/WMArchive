var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template('<div class="row"><div class="col col-12"><h1>Performance</h1><canvas id="canvas" height="300" width="500"></canvas></div></div>'),

  initialize: function() {
    this.requestPerformance();
  },

  render: function(){
    this.$el.html(this.template());

    var self = this;
    var canvas = $('canvas');
    if (self.siteCount == null) {
      canvas.hide();
    } else {
      var siteCount = self.siteCount
      var ctx = canvas.get(0).getContext("2d");
      var myBar = new Chart(ctx, {
        type: 'pie',
        data: {
          labels: siteCount.map(function(site){ return site['_id'] }),
          datasets:[ { data: siteCount.map(function(site){ return site['count'] }) } ]
        },
        options: { responsive : true, maintainAspectRatio: false }
      });
      canvas.show();
    }
  },

  requestPerformance: function() {

    this.status = 'Loading performance...';
    this.render();

    var self = this;
    $.ajax({
        url: '/wmarchive/data?site_count=1',
        contentType: "application/json",
        type: 'GET',
        cache: false,
    }).done(function(data, msg, xhr) {
        self.siteCount = data.result[0].site_count;
        self.render();
    });
  },

});
