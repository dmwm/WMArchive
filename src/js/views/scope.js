// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.ScopeView = Backbone.View.extend({

  template: _.template(`
      <form class="row">
        <div id="scope-title" class="col-md-2" style="display: flex; flex-direction: column; justify-content: center;">
          <strong class="structure">Scope</strong>
        </div>
        <div class="col-md-7" id="filters"></div>
        <div class="col-md-3 pull-sm-right" id="timeframe" style="display: flex; flex-direction: row; align-items: center"></div>
      </form>
    `),

  initialize: function() {
    // Create a filter view for every available scope filter
    this.filterViews = Object.keys(app.scope.filters).map(function(scope_key) {
      return new app.FilterView({ input_id: scope_key, label: app.scope.filters[scope_key] })
    });
    this.timeframeSelector = new app.TimeframeSelector();
  },

  render: function() {
    this.$el.html(this.template());
    // Render status view
    this.$('#scope-title').append(new app.ScopeStatusView().render().$el);
    // Render filter views
    for (var i in this.filterViews) {
      var filterView = this.filterViews[i];
      this.$('#filters').append(filterView.$el);
      filterView.render();
    }
    this.$('#timeframe').append(this.timeframeSelector.$el);
    this.timeframeSelector.render();
  },

});

// helper function to reload page with appropriate MongoDB collection, either
// day or hour
function aggLoad(col)
{
    url=window.location.href;
    if url.indexOf("agg_col") > 0 {
        url = url.replace("day", col);
        url = url.replace("hour", col);
    } else {
        url=window.location.href+'agg_col='+col;
    }
    window.location.href=url;
}

app.ScopeStatusView = Backbone.View.extend({

  className: 'status',
  template: _.template('<%=status%>'),

  initialize: function() {
    this.model = app.scope;
    this.listenTo(this.model, 'change:status', this.render);
  },

  render: function() {
    var status = this.model.get('status');
    var statusDescription = "";
    if (status == null) {
      this.$el.html('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"></div>')
    } else {
      statusDescription = "Matches <b>" + app.format_jobs(status.totalMatchedJobs) + "</b>";
      if (status.start_date != null && status.end_date != null) {
        statusDescription += "<br>from " + moment(status.start_date).format('lll') + " to " + moment(status.end_date).format('lll');
      }
      statusDescription += ".<br/>";
      b1 = " <input type=\"button\" name=\"agg_col\" value=\"Daily\" onclick=aggLoad('day')>";
      b2 = " <input type=\"button\" name=\"agg_col\" value=\"Hourly\" onclick=aggLoad('hour')>";
      statusDescription += "Collection: " + b1 + " | " + b2;
      this.$el.html(this.template({ status: statusDescription }));
    }
    return this;
  },

});
