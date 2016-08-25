var app = app || {};

app.Visualization = Backbone.Model.extend({

  urlRoot: '/wmarchive/data/performance',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'metric', 'axis'));
    var self = this;

    // Cancel pending fetches on changes
    this.pendingFetch = this.fetch();
    this.listenTo(app.scope, 'change:scope', function() {
      if (self.pendingFetch != null) {
        self.pendingFetch.abort();
        self.pendingFetch = null;
      }
      this.pendingFetch = this.fetch().complete(function() {
        self.pendingFetch = null;
      });
    });

  },

  sync: function (method, model, options) {
    this.set('data', null);

    options = options || {};
    var params = app.scope.queryParameters();
    var addParams = this.queryParameters();
    for (var param in addParams) {
      params[param] = addParams[param];
    }
    options.data = params;
    return Backbone.sync.apply(this, [method, model, options]);
  },

  parse: function(data) {
    var result = data.result[0].performance;
    return {
      data: result.visualizations[this.get('metric')][this.get('axis')],
      status: result.status,
    };
  },

  queryParameters: function() {
    return {
      metrics: [ this.get('metric') ],
      axes: [ this.get('axis') ],
    };
  },

});
