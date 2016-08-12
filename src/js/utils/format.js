var app = app || {};

app.format_jobs = function(job_count) {
  var suffix = ' jobs';
  if (job_count == 1) {
    suffix = ' job';
  }
  return app.format_jobs_tick(job_count) + suffix;
};

app.format_jobs_tick = function(job_count) {
  return numeral(job_count).format('0a');
};

app.format_time_daterangepicker = 'DD/MM/YYYY';
app.format_time_d3 = d3.timeFormat('%B %d, %Y');

app.format_value = function(metric) {
  switch (metric) {
    case 'jobstate':
      return app.format_jobs;
    case 'cpu.jobTime':
      return function(value) {
        return numeral(value).format("00:00:00") + " (" + numeral(value).format("0.0") + "s" + ")";
      };
    case 'storage.read':
    case 'storage.write':
      return function(value) {
        return numeral(value * 1e6).format("0.0 b");
      };
    default:
      return function(value) {
        return numeral(value).format("0.0");
      };
    }
};

app.format_tick = function(metric) {
  switch (metric) {
    case 'jobstate':
      return app.format_jobs_tick;
    case 'cpu.jobTime':
      return function(value) {
        return numeral(value).format("00:00:00");
      };
    case 'storage.read':
    case 'storage.write':
      return function(value) {
        return numeral(value * 1e6).format("0.0 b");
      };
    default:
      return function(value) {
        return numeral(value).format("0.0");
      };
    }
};

app.format_ticks_label = function(metric) {
  switch (metric) {
    case 'jobstate':
      return "Jobs";
    case 'cpu.jobTime':
      return "Job Time"
    case 'storage.read':
    case 'storage.write':
      return "Memory"
    default:
      return null;
    }
};
