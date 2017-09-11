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
function aggLoad(dbname, col)
{
    url=window.location.href;
    url = url.replace(/(aggDB=)[^\&]+/, '$1' + dbname)
    url = url.replace(/(aggCol=)[^\&]+/, '$1' + col)
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
    var desc = "";
    if (status == null) {
      this.$el.html('<div class="loading-indicator"><img src="/wmarchive/web/static/images/cms_loading_indicator.gif"></div>')
    } else {
      desc = "Matches <b>" + app.format_jobs(status.totalMatchedJobs) + "</b>";
      if (status.start_date != null && status.end_date != null) {
        desc += "<br>from " + moment(status.start_date).format('lll') + " to " + moment(status.end_date).format('lll');
      }
      desc += ".<br/><b>Collections</b>";
      desc += " <div id=\"fwjr_day\" name=\"fwjr_day\">WMAgent: <a href=\"javascript:aggLoad('aggregated', 'day')\">daily</a></div>"
      desc += " <div id=\"fwjr_hour\" name=\"fwjr_hour\">WMAgent: <a href=\"javascript:aggLoad('aggregated', 'hour')\">hourly</a></div>"
      desc += " <div id=\"crab_day\" name=\"crab_day\">CRAB: <a href=\"javascript:aggLoad('crab', 'day')\">daily</a></div>"
      desc += " <div id=\"crab_hour\" name=\"crab_hour\">CRAB: <a href=\"javascript:aggLoad('crab', 'hour')\">hourly</a></div>"
      this.$el.html(this.template({ status: desc }));
    }
    // adjust web page to highlight the view we are in
    highlightView();
    return this;
  },

});

// helper function to find out value of given parameter name in url
function gup( name, url ) {
    if (!url) url = location.href;
    name = name.replace(/[\[]/,"\\\[").replace(/[\]]/,"\\\]");
    var regexS = "[\\?&]"+name+"=([^&#]*)";
    var regex = new RegExp( regexS );
    var results = regex.exec( url );
    return results == null ? null : results[1];
}

// helper function to highlightView of db/collection
function highlightView() {
    // extract value of aggDB and aggCol url parameters
    var url = window.location.href;
    var aggDB = gup('aggDB', url);
    var aggCol = gup('aggCol', url);

    // reset previous settings on elements
    var elements = ['crab_day', 'crab_hour', 'fwjr_day', 'fwjr_hour'];
    for (i = 0; i < elements.length; i++) {
        var item = document.getElementById(elements[i]);
        if (item != null) {
                item.className = 'inactive-div';
        }
    }
    // identify which db/collection to highlight
    if (aggDB == 'crab') {
        item = document.getElementById('crab_'+aggCol);
    } else {
        item = document.getElementById('fwjr_'+aggCol);
    }
    if (item != null) {
       item.className = 'active-div';
    }
}
