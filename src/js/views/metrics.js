var app = app || {};

app.MetricsView = Backbone.View.extend({

  template: _.template('<strong class="structure">Metrics</strong><fieldset id="metric-selectors" class="form-group"></fieldset><strong class="structure">Axes</strong><fieldset id="axis-selectors" class="form-group"></fieldset>'),

  initialize: function() {
    this.metricSelectors = [
      new app.MetricSelector({ id: "jobstate", label: "Job State" }),
      new app.MetricSectionTitle({ title: "CPU" }),
      new app.MetricSelector({ id: "jobtime", label: "Job Time" }),
      new app.MetricSelector({ id: "jobcpu", label: "Job CPU" }),
      new app.MetricSectionTitle({ title: "Storage" }),
      new app.MetricSelector({ id: "readTotal", label: "Read Total" }),
      new app.MetricSelector({ id: "writeTotal", label: "Write Total" }),
    ];
    this.axisSelectors = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.MetricSelector({ id: scope_key, label: app.scope.filters[scope_key] });
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
    var element = event.target.id;
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
      this.$('#' + activeElement).addClass('active');
    }
    for (var activeElement of this.model.get('axes')) {
      this.$('#' + activeElement).addClass('active');
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
