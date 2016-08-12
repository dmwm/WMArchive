var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template('<div class="container" id="scope-container"><div class="row"><div class="col-sm-12" id="scope"></div></div></div><div class="container" id="main-container"><div class="row"><div class="col-sm-2" id="metrics"></div><div class="col-sm-10" id="visualizations"></div></div></div>'),

  initialize: function() {
    this.scopeView = new app.ScopeView();
    this.metricsView = new app.MetricsView();
    this.model = app.scope;
    this.model.on('change:visualizations', this.render, this);
  },

  render: function() {
    this.$el.html(this.template());
    this.scopeView.setElement(this.$('#scope')).render();
    this.metricsView.setElement(this.$('#metrics')).render();

    var self = this;
    var visualizations = self.model.get('visualizations');

    var canvas = this.$('#visualizations');

    for (var metric in visualizations) {
      for (var axis in visualizations[metric]) {
        var VisualizationView = app.visualizationViews[metric];
        if (axis === 'time') {
          VisualizationView = app.visualizationViews['time'];
        }
        if (VisualizationView == null) {
          VisualizationView = app.visualizationViews['default'];
        }
        var visualizationView = new VisualizationView({ data: visualizations[metric][axis], metric: metric, axis: axis });
        var section = new app.VisualizationSectionView().render();
        canvas.append(section.$el);
        section.$el.append('<h5>' + self.model.titleForMetric(metric) + ' per ' + axis.charAt(0).toUpperCase() + axis.slice(1) + '</h5>');
        section.$el.append(visualizationView.$el);
        visualizationView.render();
      }
    }
    $('[data-toggle="tooltip"]').tooltip(); // FIXME: Move this to an appropriate place
  },

});

app.VisualizationSectionView = Backbone.View.extend({

  tagName: 'section',
  className: 'visualization-container',

});
