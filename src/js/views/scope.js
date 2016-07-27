var app = app || {};

app.ScopeView = Backbone.View.extend({

  template: _.template('<form class="row"><div class="col-sm-2" style="display: flex; flex-direction: row; align-items: center"><strong class="structure">Scope</strong></div><div class="col-sm-7" id="filters"></div><div class="col-sm-3" id="timeframe" style="display: flex; flex-direction: row; align-items: center"></div></div>'),

  initialize: function() {
    this.filterViews = [
      new app.FilterView({ input_id: "workflow", label: "Workflow" }),
      new app.FilterView({ input_id: "task", label: "Task" }),
      new app.FilterView({ input_id: "host", label: "Host" }),
      new app.FilterView({ input_id: "site", label: "Site" }),
      new app.AddFilterButton(),
    ];
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
