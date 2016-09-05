var app = app || {};

app.format_jobs = function(job_count) {
  var suffix = ' jobs';
  if (job_count == 1) {
    suffix = ' job';
  }
  return app.format_jobs_tick(job_count) + suffix;
};

app.format_jobs_tick = function(job_count) {
  return numeral(job_count).format('0.[00]a');
};

app.format_time_daterangepicker = 'L';
app.format_time_d3 = d3.timeFormat('%B %d, %Y');

app.format_value = function(metric) {
  return function(value) {
    switch (metric) {
      case 'jobstate':
        return app.format_jobs(value);
      case 'events':
        return numeral(value).format('0.[00]a') + " events";
      case 'cpu.TotalJobTime':
          return numeral(value).format("00:00:00") + " (" + numeral(value).format("0.0") + "s" + ")";
      case 'storage.readTotalMB':
      case 'storage.writeTotalMB':
          return numeral(value * 1e6).format("0.[00] b");
      default:
          return numeral(value).format("0.[00]");
      }
    };
};

app.format_tick = function(metric) {
  switch (metric) {
    case 'jobstate':
      return app.format_jobs_tick;
    case 'cpu.TotalJobTime':
      return function(value) {
        return numeral(value).format("00:00:00");
      };
    case 'storage.readTotalMB':
    case 'storage.writeTotalMB':
      return function(value) {
        return numeral(value * 1e6).format("0.[00] b");
      };
    default:
      return function(value) {
        return numeral(value).format("0.[00]a");
      };
    }
};

app.format_ticks_label = function(metric) {
  switch (metric) {
    case 'jobstate':
      return "Jobs";
    case 'events':
      return "Events";
    case 'cpu.TotalJobTime':
      return "Processing Time"
    case 'storage.readTotalMB':
    case 'storage.writeTotalMB':
      return "Memory"
    default:
      return null;
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
