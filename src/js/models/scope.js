var app = app || {};

app.Scope = Backbone.Model.extend({

  urlRoot: '/wmarchive/data/performance',

  filters: {
    'workflow': "Workflow",
    'task': "Task",
    'host': "Host",
    'site': "Site",
    'jobtype': "Job Type",
    'jobstate': "Job State",
    'acquisitionEra': "Acquisition Era",
    'exitCode': "Exit Code",
    'exitStep': "Exit Step",
    // 'time' is handled separately
  },

  defaults: {
    metrics: [ 'jobstate' ],
    axes: [ 'host', 'jobstate', 'site' ],

    start_date: moment().subtract(7, 'days'),
    end_date: moment(),
    workflow: null,
    task: null,
    step: null,
    host: null,
    site: null,
    acquisitionEra: null,
    jobtype: null,
    jobstate: null,
    exitCode: null,
  },

  initialize: function() {
    var self = this;

    // Trigger update URL on changes
    this.on('change:scope change:metrics change:axes', this.updateURL, this);

    // Cancel pending fetches on changes
    this.pendingFetch = this.fetch();
    this.on('change:scope', function() {
      if (self.pendingFetch != null) {
        self.pendingFetch.abort();
        self.pendingFetch = null;
      }
      this.pendingFetch = this.fetch().complete(function() {
        self.pendingFetch = null;
      });
    }, this);

    // Constrain timeframe on status change
    this.on('change:status', function() {
      var status = this.get('status') || {};
      var min_date = status.min_date;
      var max_date = status.max_date;
      if (min_date != null && max_date != null) {
        min_date = moment(min_date);
        max_date = moment(max_date);
        if (!min_date.isSame(this.get('min_date')) || !max_date.isSame(this.get('max_date'))) {
          this.set({ min_date: min_date, max_date: max_date });
        }
      }
    }, this);
    this.on('change:min_date change:max_date', function() {
      var min_date = this.get('min_date');
      var max_date = this.get('max_date');
      var start_date = this.get('start_date');
      var end_date = this.get('end_date');
      if (min_date != null && start_date.isBefore(min_date)) {
        start_date = moment(min_date);
      }
      if (max_date != null && end_date.isAfter(max_date)) {
        end_date = moment(max_date);
      }
      if (start_date.isAfter(end_date)) {
        start_date = moment(min_date)
      }
      if (!start_date.isSame(this.get('start_date')) || !end_date.isSame(this.get('end_date'))) {
        this.set({ start_date: start_date, end_date: end_date });
      }
    }, this);

    // Trigger `change:scope` event on any scope change
    this.on(Object.keys(this.defaults).map(function(key) {
      if (_.contains([ 'metrics', 'axes' ], key)) {
        return '';
      }
      return 'change:' + key;
    }).join(' '), function() {
      this.trigger("change:scope");
    }, this);
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

  metricForKey: function(metric_path) {
    var metric = this.get('all_metrics');
    for (var metric_key of metric_path.split(".")) {
      if (metric == null) {
        return null;
      }
      var metrics = metric.metrics || metric;
      metric = _.find(metrics, function(m) { return m.key == metric_key; });
    }
    return metric;
  },

  sync: function (method, model, options) {
    var params = this.queryParameters();
    params['metrics'] = [];
    params['axes'] = [];
    var suggestions = Object.keys(this.filters);
    suggestions.splice(suggestions.indexOf('workflow'), 1);
    suggestions.splice(suggestions.indexOf('task'), 1);
    params['suggestions'] = suggestions;
    options.data = params;
    return Backbone.sync.apply(this, [method, model, options]);
  },

  parse: function(data) {
    var result = data.result[0].performance;
    result.all_metrics = result.supplementaryData.metrics;
    return result;
  },

});
