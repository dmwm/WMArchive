var app = app || {};

app.Visualizations = Backbone.Collection.extend({

  model: app.Visualization,
  modelId: function(attrs) {
    return attrs.metric + attrs.axis;
  },

  initialize: function() {
    this.construct();
    this.listenTo(app.scope, 'change:metrics change:axes change:all_metrics', this.construct);
  },

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
