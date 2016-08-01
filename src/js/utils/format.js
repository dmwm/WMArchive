function format_jobs(job_count) {
    if (job_count == 1) {
      return job_count + ' job';
    } else {
      return job_count + ' jobs';
    }
}
