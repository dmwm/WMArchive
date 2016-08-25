var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template(`
    <div class="container-fluid" id="scope-container">
      <div class="row">
        <div class="col-md-12" id="scope"></div>
      </div>
    </div>
    <div class="container-fluid" id="main-container">
      <div class="row">
        <div class="col-md-2" id="metrics"></div>
        <div class="col-md-10 card-columns" id="visualizations"></div>
      </div>
    </div>
  `),

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
  className: 'visualization-container card',

  template: _.template(`
    <div class="card-header"><%=title%></div>
    <div class="card-block"></div>
  `),

  initialize: function(options) {
    this.listenTo(this.model, 'change:data change:error', this.render, this);
    this.listenTo(this.model, 'destroy', function(a,b,c) {
      this.remove();
    }, this);
  },

  render: function() {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var data = this.model.get('data');
    var error = this.model.get('error');

    var title = app.scope.titleForMetric(metric);
    if (axis == 'time') {
      title += " Evolution";
    } else {
      title += " per " + app.scope.filters[axis];
    }

    this.$el.html(this.template({ title: title }));
    var content = this.$('.card-block');

    if (data == null) {
      if (error != null) {
        content.append('<p class="card-text text-xs-center"><small class="text-muted">' + error + '</small></p>')
      } else {
        content.append('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"><p><strong class="structure">Loading...</structure></p></div>');
      }

    } else {
      var VisualizationView = app.visualizationViews[metric];
      if (axis === 'time') {
        VisualizationView = app.visualizationViews['time'];
      }
      if (VisualizationView == null) {
        VisualizationView = app.visualizationViews['default'];
      }
      var visualizationView = new VisualizationView({ data: data, metric: metric, axis: axis });
      content.append(visualizationView.$el);
      visualizationView.render();

      visualizationView.$('[data-toggle="tooltip"]').tooltip();

      content.append('<p class="card-text text-xs-right"><small class="text-muted">aggregated in ' + numeral(this.model.get('status').time).format('0.00') + ' seconds</small></p>');

    }

    return this;
  },

});
