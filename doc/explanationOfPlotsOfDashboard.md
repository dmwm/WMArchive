# Dashbard Plots' Explanation

## Time & CPU DashBoard

- Total Job CPU: The total CPU spent for a job in second. This is a full account that include initial and data processing CPU time.
- Total Event CPU: The total CPU time spent processing the Events. This actually excluded time processing Runs and Lumis as well as the time it took to read the event from the source. This value also was only available in the single threaded framework.
- Avg Event CPU: The sum over the CPU time spent for each event then divided by the total number of events in a job. Note this is different from the throughput. The sum of the CPU time include every part. For example, if we have multiple threads running at the same time, we will count the CPU time for all the threads.
- Min/Max Event CPU: pick the smallest/maxium CPU time spent for an event in a job.
- Total Loop CPU: The total CPU time spent in the 'data processing loop'. I.e. time spent while reading and processing Runs, Lumis and Events. So this excludes time setting up the job and tearing down the job.
- All the time plots are the same as the crossponding CPU plots except repacing the CPU time with the wall clock time.
- Total Init Time: The wall clock time used at the begining of a job for configuration, opening files and setting up the job. This time usually is single thread.
- Number of streams: Concurrent events. In other words, the number of events are processed concurrently.
- Number of threads: The number of thresds are running. Im general, Number of streams should be the same as number of threads.
- Event Throughput: Number of events per second. When it is multiple threads, all the time spent by each thread is count one. For example, there are four threads, the time counts as 1 instead of 4.   
