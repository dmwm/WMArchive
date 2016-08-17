var app = app || {};

app.Visualization = Backbone.Model.extend({

  urlRoot: '/wmarchive/data/performance',

  initialize: function(options) {
    _.extend(this, _.pick(options, 'metric', 'axis'));
    this.fetch();
    this.listenTo(app.scope, "change:scope", this.fetch);
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
    return { data: data.result[0].performance.visualizations[this.get('metric')][this.get('axis')] };
  },

  queryParameters: function() {
    return {
      metrics: [ this.get('metric') ],
      axes: [ this.get('axis') ],
    };
  },

});
