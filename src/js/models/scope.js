var app = app || {};

app.Scope = Backbone.Model.extend({

  urlRoot: '/wmarchive/data/performance',

  filters: {
    'workflow': "Workflow",
    'task': "Task",
    'host': "Host",
    'site': "Site",
    'jobtype': "Job Type"
  },

  defaults: {
    metrics: [ 'jobstate' ],
    start_date: moment('2016-06-28'),
    end_date: moment(),
    workflow: null,
    task: null,
    host: null,
    site: null,
    jobtype: null,
    axes: [ 'workflow', 'host', 'site' ],
  },

  initialize: function() {
    this.fetch();
    for (var key in this.defaults) {
      this.on('change:' + key, function(self) {
        this.updateURL();
        this.fetch();
      });
    }
  },

  sync: function (method, model, options) {
    options = options || {};
    options.data = this.queryParameters();
    return Backbone.sync.apply(this, [method, model, options]);
  },

  parse: function(data) {
    return data.result[0].performance;
  },

  queryParameters: function() {
    var params = {};
    for (var key in this.defaults) {
      var value = this.get(key);
      if (value != null && value != '') {
        switch (key) {
          case 'start_date':
          case 'end_date':
            value = value.format('YYYYMMDD');
            break;
          default:
            break;
        }
        params[key] = value;
      }
    }
    return params;
  },

  updateURL: function() {
    var self = this;
    var params = this.queryParameters();
    app.router.navigate('/performance?' + Object.keys(params).map(function(key) {
      var value = params[key];
      if (key == 'metrics' || key == 'axes') {
        return value.map(function(element) {
          return key + '[]=' + element;
        }).join('&');
      } else {
        return key + '=' + params[key];
      }
    }).join('&'), { replace: true });
  },

  setQuery: function(query) {
    for (var key in query) {
      if (!(_.contains(Object.keys(this.defaults), key))) {
        delete query[key];
        continue;
      }
      switch (key) {
        case 'start_date':
        case 'end_date':
          query[key] = moment(query[key], 'YYYYMMDD');
          break;
        default:
          break;
      }
    }
    this.set(query);
  },

});
