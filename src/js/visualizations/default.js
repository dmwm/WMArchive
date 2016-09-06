var app = app || {};
app.visualizationViews = app.visualizationViews || {};

app.visualizationViews['default'] = Backbone.View.extend({

  title: '',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'data', 'metric', 'axis', 'supplementaryData'));
  },

  render: function() {
    this.$el.empty();

    var canvas = d3.select(this.el);

    var axis_title = app.scope.filters[this.axis];

    var data = this.data;
    data.sort(function(lhs, rhs) { return (lhs.label || "").toString().localeCompare((rhs.label || "").toString()); });

    if (data.length <= 5) {

      app.visualizationRenderers.bars(canvas, data, this.metric, this.axis, this.supplementaryData);

    } else {

      var max_container = canvas.append('section');
      max_container.append('h6').attr('class', 'canvas-title').text("Top " + axis_title + "s");
      var max_canvas = max_container.append('div')
      var map_value = function(item) {
        if (item.jobstates != null) {
          return d3.sum(item.jobstates.map(function(d) { return d.count }));
        } else {
          return item.average;
        }
      }
      var value_sorted_data = data.slice(0).sort(function(lhs, rhs) { return map_value(rhs) - map_value(lhs)  })
      app.visualizationRenderers.bars(max_canvas, value_sorted_data.slice(0, 5), this.metric, this.axis, this.supplementaryData);

      var all_container = canvas.append('section');
      all_container.append('h6').attr('class', 'canvas-title').text("All " + axis_title + "s");
      var all_canvas = all_container.append('div')

      if (data.length <= 30) {
        app.visualizationRenderers.pies(all_canvas, data, this.metric, this.axis, this.supplementaryData);
      } else {
        app.visualizationRenderers.heatmap(all_canvas, data, this.metric, this.axis, this.supplementaryData);
      }

    }

    return this;
  },

  events: {
    'click [data-scope-filter]': 'refineFilter',
  },

  refineFilter: function(event) {
    app.scope.set(this.axis, event.target.getAttribute('data-scope-filter'));
  },

});
