var app = app || {};

app.PerformanceView = Backbone.View.extend({

  template: _.template('<div class="container" id="scope-container"><div class="row"><div class="col-sm-12" id="scope"></div></div></div><div class="container" id="main-container"><div class="row"><div class="col-sm-2" id="metrics"></div><div class="col-sm-10" id="visualizations"></div></div></div>'),

  initialize: function() {
    this.scopeView = new app.ScopeView();
    this.metricsView = new app.MetricsView();
    this.model = app.scope;
    this.model.on('change:metrics', this.render, this);
  },

  render: function() {
    this.$el.html(this.template());
    this.scopeView.setElement(this.$('#scope')).render();
    this.metricsView.setElement(this.$('#metrics')).render();

    var self = this;
    var metrics = self.model.get('metrics');

    var canvas = this.$('#visualizations');

    for (var metric in metrics) {
      var visualization = new app.visualizations[metric]({ data: metrics[metric] });
      canvas.append(visualization.render().$el);
    }
  },

});
