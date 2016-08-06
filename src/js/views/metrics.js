var app = app || {};

app.MetricsView = Backbone.View.extend({

  template: _.template('<strong class="structure">Metrics</strong><fieldset id="metric-selectors" class="form-group"></fieldset><strong class="structure">Axes</strong><fieldset id="axis-selectors" class="form-group"></fieldset>'),

  initialize: function() {
    this.metricSelectors = [].concat.apply([], Object.keys(app.scope.all_metrics).map(function(metric_key) {
      var value = app.scope.all_metrics[metric_key];
      if (typeof value === 'string') {
        return [ new app.MetricSelector({ name: metric_key, label: value }) ];
      } else {
        return Object.keys(value).map(function(nested_metric_key) {
          if (nested_metric_key == '_title') {
            return new app.MetricSectionTitle({ title: value._title });
          }
          return new app.MetricSelector({ name: metric_key + '.' + nested_metric_key, label: value[nested_metric_key] });
        });
      }
    }));
    this.axisSelectors = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.MetricSelector({ name: scope_key, label: app.scope.filters[scope_key] });
    });
    this.model = app.scope;
    this.model.on('change:metrics', this.metricsChanged, this);
    this.model.on('change:axes', this.metricsChanged, this);
  },

  render: function(){
    this.$el.html(this.template());
    for (var selector of this.metricSelectors) {
      this.$('#metric-selectors').append(selector.$el);
      selector.render();
    }
    for (var selector of this.axisSelectors) {
      this.$('#axis-selectors').append(selector.$el);
      selector.render();
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
  },

});
