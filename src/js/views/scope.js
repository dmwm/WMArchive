var app = app || {};

app.ScopeView = Backbone.View.extend({

  template: _.template('<form class="row"><div id="scope-title" class="col-sm-2" style="display: flex; flex-direction: column; justify-content: center;"><strong class="structure">Scope</strong></div><div class="col-sm-7" id="filters"></div><div class="col-sm-3" id="timeframe" style="display: flex; flex-direction: row; align-items: center"></div></div>'),

  initialize: function() {
    this.filterViews = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.FilterView({ input_id: scope_key, label: app.scope.filters[scope_key] })
    });
    this.timeframeSelector = new app.TimeframeSelector();
  },

  render: function() {
    this.$el.html(this.template());
    this.$('#scope-title').append(new app.ScopeStatusView().render().$el);
    for (var i in this.filterViews) {
      var filterView = this.filterViews[i];
      this.$('#filters').append(filterView.$el);
      filterView.render();
    }
    this.$('#timeframe').append(this.timeframeSelector.$el);
    this.timeframeSelector.render();
  },

});

app.ScopeStatusView = Backbone.View.extend({

  className: 'status',
  template: _.template('<%=status%>'),

  initialize: function() {
    this.model = app.scope;
    this.listenTo(this.model, 'change:status', this.render);
  },

  render: function() {
    var status = this.model.get('status');
    if (status != null) {
      status = "Matches <b>" + app.format_jobs(status.totalMatchedJobs) + "</b><br>from " + moment(status.start_date).format('lll') + " to " + moment(status.end_date).format('lll') + "."
    }
    this.$el.html(this.template({ status: status }));
    return this;
  },

});
