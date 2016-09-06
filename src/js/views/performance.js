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
        <div class="col-md-10">
          <div id="summary"></div>
          <div class="card-columns" id="visualizations"></div>
        </div>
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

    this.$('#summary').append(new app.SummaryVisualizationSectionView({ model: app.summary }).render().$el);

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
    this.listenTo(this.model, 'destroy', function() {
      this.remove();
    }, this);
  },

  render: function() {
    var data = this.model.get('data');

    this.$el.html(this.template({ title: this.title() }));
    var container = this.$('.card-block');

    if (data == null) {
      this.renderPlaceholder(container);
    } else {
      var content = this.renderData(container);

      content.$('[data-toggle="tooltip"]').tooltip();

      container.append('<p class="card-text text-xs-right"><small class="text-muted">aggregated in ' + numeral(this.model.get('status').time).format('0.00') + ' seconds</small></p>');
    }

    return this;
  },

  title: function() {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var title = app.scope.titleForMetric(metric);
    if (axis == 'time') {
      title += " Evolution";
    } else {
      title += " per " + app.scope.filters[axis];
    }
    return title;
  },

  renderPlaceholder: function(container) {
    var error = this.model.get('error');
    if (error != null) {
      container.append('<p class="card-text text-xs-center"><small class="text-muted">' + error + '</small></p>')
    } else {
      container.append('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"><p><strong class="structure">Loading...</structure></p></div>');
    }
  },

  renderData: function(container) {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var data = this.model.get('data');

    var VisualizationView = app.visualizationViews[metric];
    if (axis === 'time') {
      VisualizationView = app.visualizationViews['time'];
    }
    if (VisualizationView == null) {
      VisualizationView = app.visualizationViews['default'];
    }
    var visualizationView = new VisualizationView({ data: data, metric: metric, axis: axis, supplementaryData: this.model.get('supplementaryData') });
    container.append(visualizationView.$el);
    return visualizationView.render();
  }

});

app.SummaryVisualizationSectionView = app.VisualizationSectionView.extend({

  title: function() {
    return "Summary";
  },

  renderData: function(container) {
    var data = this.model.get('data');

    var container = d3.select(container.get(0));

    if (data['jobstate'] != null) {
      var value = data['jobstate'];
      var total_count = d3.sum(value.jobstates.map(function(d) { return d.count }));
      var success_count = 0;
      for (var d of value.jobstates) {
        if (d.jobstate == 'success') {
          success_count = d.count;
          break;
        }
      }
      value['label'] = numeral(success_count/total_count).format('0.00%') + " success rate";
      app.visualizationRenderers.bars(container, [ value ], metric, '_summary');
    }
    for (var metric in data) {
      if (metric == 'jobstate') {
        continue;
      }
      var title = app.scope.titleForMetric(metric);
      var value = data[metric];
      container.append('p').text(app.format_value(metric)(value.average) + " " + title);
    }

    return this;
  },

});
