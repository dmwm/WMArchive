var app = app || {};
app.visualizationViews = app.visualizationViews || {};

app.visualizationViews['default'] = Backbone.View.extend({

  title: '',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'title'));
  },

  render: function() {
    this.$el.empty();

    var self = this;

    var data = this.data;

    var canvas = d3.select(this.el);

    var container = canvas.selectAll('.full-width-container')
      .data(data)
      .enter()
        .append('div').attr('class', 'full-width-container');

    var label = container.append('text')
      .attr('class', 'chart-label')
      .text(function(d) {
        return d['label'];
      });

    var length = d3.scaleLinear()
      .range([ 0, 100 ])
      .domain([ 0, d3.max(data, function(d) { return d.average; }) ]);

    var bar = container.append('svg')
      .attr('class', 'chart')
      .append('rect')
        .attr('height', '100%')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', function(d) {
          return length(d.average) + '%';
        })

    return this;
  },

});
