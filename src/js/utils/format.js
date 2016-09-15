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
  return function(value) {
    switch (metric) {
      case 'jobstate':
        return numeral(value).format('0.[00]a');
      case 'storage.readPercentageOps':
      case 'storage.readCachePercentageOps':
        return numeral(value / 100).format('0.[00]%');
      case 'storage.readAverageKB':
        return numeral(value * 1e3).format('0.[00]b');
      case 'storage.readNumOps':
        return numeral(value).format("0.[00]a");
      case 'storage.readMBSec':
        return numeral(value * 1e6).format("0.[00]b") + "/ms";
      case 'storage.readMaxMSec':
        return numeral(value).format("0.[00]a") + " ms";
      case 'storage.readTotalSecs':
      case 'storage.writeTotalSecs':
        return numeral(value).format("00:00:00");
      default:
        if (metric.startsWith('cpu.') && metric != 'cpu.EventThroughput') {
          return numeral(value).format("00:00:00");
        } else if (metric.startsWith('storage.') || metric.startsWith('memory.')) {
          return numeral(value * 1e6).format("0.[00] b");
        } else {
          return numeral(value).format("0.[00]a");
        }
      }
    };
};

app.format_value = function(metric) {
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
      case 'cpu.EventThroughput':
        return tick + " events/second";
      default:
        if (metric.startsWith('cpu.')) {
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
