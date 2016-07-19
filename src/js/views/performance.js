var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template('<div class="container" id="scope-container"><div class="row"><div class="col-sm-12" id="scope"></div></div></div><div class="container" id="main-container"><div class="row"><div class="col-sm-2" id="metrics"></div><div class="col-sm-10" id="visualizations"></div></div></div>'),

  initialize: function() {
    this.scopeView = new app.ScopeView();
    this.metricsView = new app.MetricsView();
    this.model = app.scope;
    this.model.on('change:metrics', this.render, this);
  },

  render: function() {
    this.$el.html(this.template());
    this.scopeView.setElement(this.$('#scope')).render();
    this.metricsView.setElement(this.$('#metrics')).render();

    var self = this;
    var metrics = self.model.get('metrics');

    if (metrics != null) {
      var siteCount = metrics.jobstatePerSite;
      var sites = siteCount.map(function(site) { return site['_id'] })
      var counts = siteCount.map(function(site) { return d3.sum(site['jobstates'].map(function(stateData) { return stateData['count'] })) });
      var maxCount = d3.max(counts);

      var canvas = d3.select('#visualizations');

      var container = canvas.selectAll('.site-count-container')
        .data(siteCount)
        .enter()
          .append('div').attr('class', 'site-count-container')
      var pie = container.append('svg')
        .attr('class', 'chart')
        .attr('width', function(site) {
          return Math.sqrt(d3.sum(site['jobstates'].map(function(stateData) { return stateData['count'] })) / maxCount) * 200 + 'px';
        })
        .attr('viewBox', '0 0 100 100')
        .append("g")
          .attr("transform", 'translate(50,50)')
        // .append('circle')
        //   .attr('fill', '#DB392E')
        //   .attr('r', '50');
      var pieLayout = d3.pie()
        .value(function(d) {
          return d.count;
        })
        .sort(null);
      var path = pie.selectAll('path')
        .data(function(d) {
          return pieLayout(d.jobstates);
        })
        .enter()
          .append('path')
          .attr('d', d3.arc()
            .innerRadius(50 / 4)
            .outerRadius(50))
          .attr('fill', function(d, i) {
            switch (d.data.jobstate) {
              case 'success':
                return '#31AD64';
              case 'jobfailed':
                return '#E54E42';
              case 'submitfailed':
                return '#3081B8';
              default:
                return 'black';
            }
          });

      // var stack = container.append('svg')
      //   .attr('width', '200px')
      //   .attr('height', '30px')
      //   .attr('viewBox', function(d) {
      //     return '0 0 ' + d3.sum(d.jobstates.map(function(jobstate) { return jobstate.count })) + ' 100';
      //   });
      // stack.selectAll('rect')
      //   .data(function(d) {
      //     return d.jobstates;
      //   })
      //   .enter()
      //     .append('rect')
      //     .attr('y', '0')
      //     .attr('x', function(d, i) {
      //       return 0;
      //     })
      //     .attr('width', function(d) {
      //       return d.count;
      //     })
      //     .attr('height', '100')
      //     .attr('fill', function(d) {
      //       switch (d.jobstate) {
      //         case 'success':
      //           return '#31AD64';
      //         case 'jobfailed':
      //           return '#E54E42';
      //         case 'submitfailed':
      //           return '#3081B8';
      //         default:
      //           return 'black';
      //       }
      //     });

      var label = container.append('text')
        .attr('class', 'chart-label')
        .text(function(site) {
          return site['_id'];
        })

      // var ctx = canvas.get(0).getContext("2d");
      // var myBar = new Chart(ctx, {
      //   type: 'pie',
      //   data: {
      //     labels: siteCount.map(function(site){ return site['_id'] }),
      //     datasets:[ { data: siteCount.map(function(site){ return site['count'] }) } ]
      //   },
      //   options: { responsive : true, maintainAspectRatio: false }
      // });
      // canvas.show();
    }
  },

});
