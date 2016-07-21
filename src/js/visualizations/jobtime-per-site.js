var app = app || {};
app.visualizations = app.visualizations || {};

app.visualizations['jobtimePerSite'] = Backbone.View.extend({

  id: 'jobtimePerSite',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data'));
  },

  render: function() {
    this.$el.empty();

    var self = this;

    var data = this.data;
    // var sites = data.map(function(site) { return site['_id'] })
    // var counts = data.map(function(site) { return d3.sum(site['jobstates'].map(function(stateData) { return stateData['count'] })) });
    // var max = d3.max(counts);

    var canvas = d3.select(this.el);

    var container = canvas.selectAll('.full-width-container')
      .data(data)
      .enter()
        .append('div').attr('class', 'full-width-container');

    var label = container.append('text')
      .attr('class', 'chart-label')
      .text(function(d) {
        return d['site'];
      });

    var length = d3.scaleLinear()
      .range([ 0, 100 ])
      .domain([ 0, d3.max(data, function(d) { return d.averageJobTime; }) ]);

    var bar = container.append('svg')
      .attr('class', 'chart')
      .append('rect')
        .attr('height', '100%')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', function(d) {
          return length(d.averageJobTime) + '%';
        })

    return this;
  },

});
