var app = app || {};

app.ScopeView = Backbone.View.extend({

  template: _.template('<form class="row"><div class="col-sm-2" style="display: flex; flex-direction: row; align-items: center"><strong class="structure">Scope</strong></div><div class="col-sm-7" id="filters"></div><div class="col-sm-3" id="timeframe" style="display: flex; flex-direction: row; align-items: center"></div></div>'),

  initialize: function() {
    this.filterViews = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.FilterView({ input_id: scope_key, label: app.scope.filters[scope_key] })
    });
    this.timeframeSelector = new app.TimeframeSelector();
  },

  render: function() {
    this.$el.html(this.template());
    for (var i in this.filterViews) {
      var filterView = this.filterViews[i];
      this.$('#filters').append(filterView.render().$el);
    }
    this.timeframeSelector.setElement(this.$('#timeframe')).render();
  },

});
