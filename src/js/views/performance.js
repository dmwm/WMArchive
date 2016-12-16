// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

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
    // Here we create the view objects on initialization to `render` them later.
    // We can alternatively recreate them on every `render` as done in other views.
    this.scopeView = new app.ScopeView();
    this.metricsView = new app.MetricsView();

    // Observe changes to the visualizations collection and render them.
    // Note that visualizations that are `destroy`ed are removed automatically.
    this.model = app.visualizations;
    this.listenTo(this.model, 'add', this.addVisualization);
  },

  render: function() {
    this.$el.html(this.template());
    // Render views to specific elements in the HTML tree.
    // Often, views are rendered and `append`ed to the HTML tree instead, like below.
    this.scopeView.setElement(this.$('#scope')).render();
    this.metricsView.setElement(this.$('#metrics')).render();

    var self = this;

    // Always render the summary view
    this.$('#summary').append(new app.SummaryVisualizationSectionView({
      model: app.summary
    }).render().$el);

    // Render all visualizations in the collection
    self.model.each(function(visualization) {
      self.addVisualization(visualization);
    });
  },

  addVisualization: function(visualization) {
    var visualizationsView = this.$('#visualizations');
    visualizationsView.append(new app.VisualizationSectionView({
      model: visualization
    }).render().$el);
  }

});


// The container view for each visualization widget
app.VisualizationSectionView = Backbone.View.extend({

  tagName: 'section',
  className: 'visualization-container card',

  template: _.template(`
    <div class="card-header"><%=title%></div>
    <div class="card-block"></div>
  `),

  // Expected to be initialized with a `model: visualization`.
  initialize: function() {
    // Observe visualization data changes to display loading indicators and errors
    this.listenTo(this.model, 'change:data change:error', this.render, this);
    // Remove this widget when the visualization is destroyed.
    this.listenTo(this.model, 'destroy', function() {
      this.remove();
    }, this);
  },

  render: function() {
    var data = this.model.get('data');

    // Render content template
    this.$el.html(this.template({ title: this.title() }));
    var container = this.$('.card-block');

    // Render visualization when there is data available, or the placeholder otherwise
    if (data == null) {
      this.renderPlaceholder(container);
    } else {
      var content = this.renderData(container);

      content.$('[data-toggle="tooltip"]').tooltip();

      container.append('<p class="card-text text-xs-right"><small class="text-muted">aggregated in ' + numeral(this.model.get('status').time).format('0.00') + ' seconds</small></p>');
    }

    return this;
  },

  // The title of the widget that may be overridden.
  title: function() {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var title = app.scope.metricForKey(metric).title;
    if (axis == 'time') {
      title += " Evolution";
    } else {
      title += " per " + app.scope.filters[axis];
    }
    return title;
  },

  // Render the loading indicator or an error
  renderPlaceholder: function(container) {
    var error = this.model.get('error');
    if (error != null) {
      container.append('<p class="card-text text-xs-center"><small class="text-muted">' + error + '</small></p>')
    } else {
      container.append('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"><p><strong class="structure">Loading...</structure></p></div>');
    }
    return this;
  },

  // Render the actual visualization with the available data
  renderData: function(container) {
    var metric = this.model.get('metric');
    var axis = this.model.get('axis');
    var data = this.model.get('data');

    // The visualization rendering is delegated to the VisualizationView.
    var visualizationView = new app.VisualizationView({
      data: data,
      metric: metric,
      axis: axis,
      supplementaryData: this.model.get('supplementaryData')
    });
    container.append(visualizationView.$el);
    return visualizationView.render();
  }

});


// A subclass of the VisualizationSectionView that specifically renders the summary.
app.SummaryVisualizationSectionView = app.VisualizationSectionView.extend({

  title: function() {
    return "Summary";
  },

  initialize: function() {
    app.SummaryVisualizationSectionView.__super__.initialize.apply(this, arguments)
    $(window).on('resize.resizeview', this.resize.bind(this));
    this.listenTo(app.scope, 'change:all_metrics', this.render, this);
  },

  renderData: function(container) {

    if (app.scope.get('all_metrics') == null) {
      return this.renderPlaceholder(container);
    }

    var data = this.model.get('data');

    var container = d3.select(container.get(0));

    // Overall success rate
    if (data['jobstate'] != null) {
      var success_section = container.append('section');
      var value = data['jobstate']['_summary'];
      var total_count = d3.sum(value.jobstates.map(function(d) { return d.count }));
      var success_count = 0;
      for (var d of value.jobstates) {
        if (d.jobstate == 'success') {
          success_count = d.count;
          break;
        }
      }
      value['label'] = numeral(success_count/total_count).format('0.00%') + " overall success rate";
      app.visualizationRenderers.bars(success_section, [ value ], {
        metric: metric,
        axis: '_summary',
      });
    }

    var metrics = Object.keys(data);
    var i = metrics.indexOf('jobstate');
    if (i >= 0) {
      metrics.splice(i, 1);
      metrics.splice(0, 0, 'jobstate');
    }

    for (var metric of metrics) {
      var section = container.append('section');
      var title_container = section.append('h6').text(app.scope.metricForKey(metric).title + " Evolution");
      if (metric != 'jobstate') {
        title_container.append('small').attr('class', 'pull-xs-right text-muted')
          .text(app.format_value(metric)(data[metric]['_summary'].average) + ' overall average');
      }
      var canvas = section.append('div');
      app.visualizationRenderers.time(canvas, data[metric]['time'], {
        metric: metric,
        axis: 'time',
        supplementaryData: this.model.get('supplementaryData'),
      });
    }

    return this;
  },

  resize: function() {
    this.render();
  },

});
