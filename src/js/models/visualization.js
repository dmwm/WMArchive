// Author: [Nils Leif Fischer](https://github.com/knly)
// Documentation: https://github.com/knly/WMArchiveAggregation

var app = app || {};

app.Visualization = Backbone.Model.extend({

  // Specifies the URL from where to fetch data
  urlRoot: '/wmarchive/data/performance',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'metric', 'axis'));
    var self = this;

    // Initial fetch
    this.pendingFetch = this.fetch({ error: self.fetchError });

    // Cancel pending fetches on changes
    this.listenTo(app.scope, 'change:scope', function() {
      if (self.pendingFetch != null) {
        self.pendingFetch.abort();
        self.pendingFetch = null;
      }
      self.pendingFetch = this.fetch({ error: self.fetchError }).complete(function() {
        self.pendingFetch = null;
      });
    });

  },

  // Called whenever a fetch is about to happen to prepare the query
  sync: function (method, model, options) {
    // Reset the data
    // This triggeres a change event that makes the corresponding view
    // display the loading indicator
    this.set('data', null);

    // Obtain scope query parameters and combine them with the metric and axis
    // from this visualization
    options = options || {};
    var params = app.scope.queryParameters();
    var addParams = this.queryParameters();
    for (var param in addParams) {
      params[param] = addParams[param];
    }
    options.data = params;

    return Backbone.sync.apply(this, [method, model, options]);
  },

  // Called to transform the data returned from the fetch to a dictionary
  // that is then set on this object. Every key in the dictionary is set on this
  // object as an attribute.
  parse: function(data) {
    // Drill down to the actual data structure
    // as detailed in https://github.com/knly/WMArchiveAggregation/
    var result = data.result[0].performance;
    // Retrieve the data to be set on this object
    return {
      data: result.visualizations[this.get('metric')][this.get('axis')],
      status: result.status,
      error: null,
      supplementaryData: result.supplementaryData,
    };
  },

  queryParameters: function() {
    return {
      metrics: [ this.get('metric') ],
      axes: [ this.get('axis') ],
    };
  },

  fetchError: function(model, response, options) {
    if (response.readyState == 4 && response.status != 200) {
      model.set({ data: null, error: response.statusText });
    }
  },

});
