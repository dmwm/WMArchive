var app = app || {};

app.format_jobs_tick = function(job_count) {
  return app.format_tick('jobstate')(job_count);
};

app.format_jobs = function(job_count) {
  return app.format_value('jobstate')(job_count);
};

app.format_time_daterangepicker = 'L';
app.format_time_d3 = d3.timeFormat('%B %d, %Y');

app.format_tick = function(metric) {
  var unit = app.scope.metricForKey(metric).unit;
  return function(value) {
    switch (unit) {
      case "s":
        return numeral(value).format("00:00:00");
      case "%":
        return numeral(value / 100).format('0.[00]%');
      case "MB":
        return numeral(value * 1e6).format("0.[00] b");
      case "KB":
        return numeral(value * 1e3).format("0.[00] b");
      case "MB/ms":
        return numeral(value * 1e6).format("0.[00] b") + "/ms";
      case "ms":
        return numeral(value).format("0.[00]a") + " ms";
      case "microseconds":
        return numeral(value * 1e-6).format("00:00:00");
      default:
        return numeral(value).format("0.[00]a") + unit;
      }
    };
};

app.format_value = function(metric) {
  var unit = app.scope.metricForKey(metric).unit;
  return function(value) {
    var tick = app.format_tick(metric)(value);
    switch (metric) {
      case 'jobstate':
        var suffix = ' jobs';
        if (value == 1) {
          suffix = ' job';
        }
        return tick + suffix;
      case 'events':
        return tick + " events";
      default:
        if (unit == "s") {
          return tick + " (" + numeral(value).format("0.0") + "s" + ")";
        } else {
          return tick;
        }
      }
    };
};

app.format_ticks_label = function(metric) {
  switch (metric) {
    case 'jobstate':
      return "Jobs";
    default:
      return app.scope.titleForMetric(metric);
    }
};

app.format_axis_label = function(axis) {
  return function(value) {
    if (value == null) {
      return "No " + app.scope.filters[axis];
    }
    return value;
  };
}
