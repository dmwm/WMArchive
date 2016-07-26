var app = app || {};

app.Scope = Backbone.Model.extend({

  urlRoot: '/wmarchive/data/performance',

  defaults: {
    metrics: [ 'jobstate' ],
    start_date: moment('2016-06-28'),
    end_date: moment(),
    host: null,
    site: null,
  },

  initialize: function() {
    this.fetch();
    for (var key of [ 'metrics', 'start_date', 'end_date', 'host', 'site' ]) {
      this.on('change:' + key, function(self) {
        this.fetch();
      });
    }
  },

  sync: function (method, model, options) {
    options = options || {};
    options.data = {};
    for (var key of [ 'metrics', 'start_date', 'end_date', 'host', 'site' ]) {
      var value = this.get(key);
      if (value != null && value != '') {
        switch (key) {
          case 'start_date':
          case 'end_date':
            value = value.format('YYYYMMDD');
          default:
            break;
        }
        options.data[key] = value;
      }
    }
    return Backbone.sync.apply(this, [method, model, options]);
  },

  parse: function(data) {
    return data.result[0].performance;
  },

});

app.scope = new app.Scope();
