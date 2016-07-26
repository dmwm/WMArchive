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

    for (var visualization in visualizations) {
      var VisualizationView = app.visualizationViews[visualization];
      if (VisualizationView != null) {
        var visualizationView = new VisualizationView({ data: visualizations[visualization] });
        var section = new app.VisualizationSectionView().render();
        section.$el.append('<h5>' + visualizationView.title + '</h5>');
        section.$el.append(visualizationView.render().$el);
        canvas.append(section.$el);
        $('[data-toggle="tooltip"]').tooltip(); // FIXME: Move this to an appropriate place
      }
    }
  },

});

app.VisualizationSectionView = Backbone.View.extend({

  tagName: 'section',
  className: 'visualization-container',

});
