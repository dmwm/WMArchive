// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.Visualizations = Backbone.Collection.extend({

  model: app.Visualization,
  modelId: function(attrs) {
    return attrs.metric + attrs.axis;
  },

  initialize: function() {
    // Construct initial visualizations
    this.construct();
    // Listen to metrics and axes changes to add or remove visualizations dynamically
    this.listenTo(app.scope, 'change:metrics change:axes change:all_metrics', this.construct);
  },

  // Construct the list of visualizations to display, adding or removing visualizations
  // depending on the metrics and axes set on the `app.scope` singleton, that the user selected.
  construct: function() {
    var scope = app.scope;
    if (scope.get('all_metrics') == null) {
      return;
    }
    var metrics = scope.get('metrics');
    var axes = scope.get('axes');
    var self = this;
    _.each(_.clone(this.models), function(visualization) {
      if (!(_.contains(metrics, visualization.get('metric')) && _.contains(axes, visualization.get('axis')))) {
        visualization.destroy();
      }
    });
    for (var metric of metrics) {
      for (var axis of axes) {
        var visualization = _.findWhere(this, { metric: metric, axis: axis });
        if (visualization == null) {
          visualization = self.add({ metric: metric, axis: axis });
        }
      }
    }
  },

});
