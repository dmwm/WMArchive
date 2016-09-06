var app = app || {};

app.Summary = app.Visualization.extend({

  initialize: function(options) {
    var self = this;

    // Cancel pending fetches on changes
    this.pendingFetch = this.fetch({ error: self.fetchError });
    this.listenTo(app.scope, 'change:scope change:metrics', function() {
      if (self.pendingFetch != null) {
        self.pendingFetch.abort();
        self.pendingFetch = null;
      }
      self.pendingFetch = this.fetch({ error: self.fetchError }).complete(function() {
        self.pendingFetch = null;
      });
    });

  },

  parse: function(data) {
    var result = data.result[0].performance;
    var data = {};
    for (var metric in result.visualizations) {
      data[metric] = result.visualizations[metric]['_summary'];
    }
    return {
      data: data,
      status: result.status,
      error: null,
      supplementaryData: result.supplementaryData,
    };
  },

  queryParameters: function() {
    return {
      metrics: app.scope.get('metrics'),
      axes: [ '_summary' ],
    };
  },

});
