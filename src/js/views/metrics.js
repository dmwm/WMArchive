var app = app || {};

app.MetricsView = Backbone.View.extend({

  template: _.template('<strong class="structure">Metrics</strong><fieldset id="metric-selectors" class="form-group"></fieldset>'),

  initialize: function() {
    this.metricSelectors = [
      new app.MetricSelector({ id: "jobstate", label: "Job State" }),
      new app.MetricSectionTitle({ title: "CPU" }),
      new app.MetricSelector({ id: "totalJobTime", label: "Total Job Time" }),
      new app.MetricSelector({ id: "totalJobCPU", label: "Total Job CPU" }),
      new app.MetricSectionTitle({ title: "Storage" }),
      new app.MetricSelector({ id: "readTotal", label: "Read Total" }),
      new app.MetricSelector({ id: "writeTotal", label: "Write Total" }),
    ];
    this.model = app.scope;
    this.model.on('change:metrics', this.metricsChanged, this);
  },

  render: function(){
    this.$el.html(this.template());
    for (var i in this.metricSelectors) {
      var metricSelector = this.metricSelectors[i];
      this.$('#metric-selectors').append(metricSelector.$el);
      metricSelector.render();
    }
    this.metricsChanged();
  },

  events: {
    'click .metric-selector': 'toggleActive',
  },

  toggleActive: function(event) {
    var metrics = _.clone(app.scope.get('metrics'));
    var metric = event.target.id;
    var i = metrics.indexOf(metric)
    if (i >= 0) {
      metrics.splice(i, 1);
    } else {
      metrics.push(metric);
    }
    app.scope.set({ metrics: metrics });
  },

  metricsChanged: function() {
    this.$('.active').removeClass('active');
    for (var metric of this.model.get('metrics')) {
      this.$('#' + metric).addClass('active');
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
