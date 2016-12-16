// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.MetricsView = Backbone.View.extend({

  template: _.template('<strong class="structure">Metrics</strong><fieldset id="metric-selectors" class="form-group"></fieldset><strong class="structure">Axes</strong><fieldset id="axis-selectors" class="form-group"></fieldset>'),

  initialize: function() {
    this.model = app.scope;
    this.listenTo(this.model, 'change:all_metrics', this.render);
    this.listenTo(this.model, 'change:metrics', this.metricsChanged);
    this.listenTo(this.model, 'change:axes', this.metricsChanged);
  },

  render: function(){
    var all_metrics = app.scope.get('all_metrics');

    var metricSelectors = [].concat.apply([], (all_metrics || []).map(function(metric) {
      if (metric.metrics == null) {
        return [ new app.MetricSelector({ name: metric.key, label: metric.title, description: metric.description }) ];
      } else {
        var selectors = [ new app.MetricSectionTitle({ title: metric.title }) ];
        selectors.push.apply(selectors, metric.metrics.map(function(nested_metric) {
          var key = metric.key + '.' + nested_metric.key;
          return new app.MetricSelector({ name: key, label: nested_metric.title, description: nested_metric.description });
        }));
        return selectors;
      }
    }));
    var axisSelectors = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.MetricSelector({ name: scope_key, label: app.scope.filters[scope_key] });
    });


    this.$el.html(this.template());
    if (all_metrics == null) {
      // render loading indicator
      this.$('#metric-selectors').append('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"></div>');
    } else {
      for (var selector of metricSelectors) {
        this.$('#metric-selectors').append(selector.render().$el);
      }
    }
    for (var selector of axisSelectors) {
      this.$('#axis-selectors').append(selector.render().$el);
    }
    this.metricsChanged();
  },

  events: {
    'click .metric-selector': 'toggleActive',
  },

  toggleActive: function(event) {
    var element = event.target.name.replace("__", ".");
    switch (event.target.parentElement.id) {
      case 'metric-selectors':
        var key = 'metrics';
        break;
      case 'axis-selectors':
        var key = 'axes';
        break;
    }
    var activeElements = _.clone(app.scope.get(key));
    var i = activeElements.indexOf(element)
    if (i >= 0) {
      activeElements.splice(i, 1);
    } else {
      activeElements.push(element);
    }
    app.scope.set(key, activeElements);
  },

  metricsChanged: function() {
    this.$('.active').removeClass('active');
    for (var activeElement of this.model.get('metrics')) {
      this.$('#metric-selectors button[name=' + activeElement.replace(".", "__") + ']').addClass('active');
    }
    for (var activeElement of this.model.get('axes')) {
      this.$('#axis-selectors button[name=' + activeElement.replace(".", "__") + ']').addClass('active');
    }
  }

});

app.MetricSectionTitle = Backbone.View.extend({

  tagName: 'span',
  className: 'metric-section-title',

  template: _.template('<%=title%>'),

  initialize: function(params) {
    this.title = params.title;
  },

  render: function(){
    this.$el.html(this.template({ title: this.title }));
    return this;
  },

});
