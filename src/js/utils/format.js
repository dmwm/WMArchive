function format_jobs(job_count) {
  var suffix = ' jobs';
  if (job_count == 1) {
    suffix = ' job';
  }
  return numeral(job_count).format('0a') + suffix;
}
