var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template('<div class="container-fluid" id="scope-container"><div class="row"><div class="col-sm-12" id="scope"></div></div></div><div class="container-fluid" id="main-container"><div class="row"><div class="col-sm-2" id="metrics"></div><div class="col-sm-10" id="visualizations"></div></div></div>'),

  initialize: function() {
    this.scopeView = new app.ScopeView();
    this.metricsView = new app.MetricsView();
    this.model = app.visualizations;
    this.listenTo(this.model, 'add', this.addVisualization);
  },

  render: function() {
    this.$el.html(this.template());
    this.scopeView.setElement(this.$('#scope')).render();
    this.metricsView.setElement(this.$('#metrics')).render();

    var self = this;

    self.model.each(function(visualization) {
      self.addVisualization(visualization);
    });
  },

  addVisualization: function(visualization) {
    var visualizationsView = this.$('#visualizations');
    visualizationsView.append(new app.VisualizationSectionView({ model: visualization }).render().$el);
  }

});

app.VisualizationSectionView = Backbone.View.extend({

  tagName: 'section',
  className: 'visualization-container',

  initialize: function(options) {
    this.listenTo(this.model, 'change:data', this.render, this);
    this.listenTo(this.model, 'destroy', function(a,b,c) {
      this.remove();
    }, this);
  },

  render: function() {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var data = this.model.get('data');

    this.$el.empty();

    var title = app.scope.titleForMetric(metric);
    if (axis == 'time') {
      title += " Evolution";
    } else {
      title += " per " + app.scope.filters[axis];
    }

    this.$el.append('<h5>' + title + '</h5>');

    if (data == null) {
      this.$el.append('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"><p><strong class="structure">Loading...</structure></p></div>');

    } else {
      var VisualizationView = app.visualizationViews[metric];
      if (axis === 'time') {
        VisualizationView = app.visualizationViews['time'];
      }
      if (VisualizationView == null) {
        VisualizationView = app.visualizationViews['default'];
      }
      var visualizationView = new VisualizationView({ data: data, metric: metric, axis: axis });
      this.$el.append(visualizationView.$el);
      visualizationView.render();
      $('[data-toggle="tooltip"]').tooltip(); // FIXME: Move this to an appropriate place
    }

    return this;
  },

});
