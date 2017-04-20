# Aggregation procedure

- WMArchive buffers incoming FWJRs in a short-term MongoDB storage layer and then regularly moves them to its long-term HDFS storage layer. Since queries over the HDFS must be scheduled and can take a significant amount of time, an aggregation procedure is necessary to cache the FWJR performance data in the desired granularity for visualization.
- The performance data produced by the aggregation procedure has the nature of a cache: No original data is stored, instead the database of FWJRs acts as the datasource. Thus all data produced by the aggregation procedure must be re-creatable deterministically, i.e. one must be able to delete any part of the data and reproduce it consistently with the aggregation procedure.
- The cache database must remain performant and provide short access times to keep the visualization UI responsive. This means a compromise must be found between performance and the amount and granularity of the data. The latter is affected by:
  - The maximum time that data is kept. This can always be reduced by deleting old data, but it may be desireble to keep a long history of data for visualization. Remember it is always possible to generate data for specific timeframes on-demand.
  - The temporal granularity. Performance data can be aggregated over arbitrary timeframes, for example hourly, daily, weekly or monthly. This has a great effect on performance and thus must be adjusted carefully. One possibility is to regularly clean old data and replace it with data of lower granularity. This process can be scheduled regularly. A sample procedure is already implemented in `WMArchive.Tools.clean_performance`.
  - The choice of scope filters to group by, such as `host` and `site`. Users will only be able to filter by these selected attributes, but adding more attributes to the scope has a multiplicative effect on the data size. Therefore evaluating the required set of scope filters is crucial to find a compromise between functionality and performance.

## FWJR structure and `steps`

- Each FWJR such as [this](../sample_data/FWJR.json) represents a single job where most of the information, and particularly the performance data, is contained in its list of `steps`.
- A step is always of one of several types. The visualization below shows their distribution over ~5.31m individual steps:

  ![Steps](../images/009/steps.png)

  Clearly, most steps are of the `"cmsRun"`, `"stageOut"` and `"logArch"` types.

  Refer to [Report 009](../009_2016-09-02.md#handling-fwjr-steps-in-the-aggregation-procedure) for a detailed discussion on steps and their role in the aggregation procedure.

  Following up on this in [Report 010](010_2016-09-09.md#aggregation-procedure), a decision has been made to extract all required information from a job's list of steps during the aggregation procedure and then aggregate over jobs.
- To this end the aggregation procedure must operate in two phases:
  1. For each job extract both the scope values as well as the performance metrics from the list of steps, possibly by combining metrics from multiple steps.
  2. Aggregate performance metrics over all jobs grouped by their scope values.
- Since the resulting data is highly dependent on the implementation of this algorithm, it must be carefully constructed. In particular, any assumptions on the FWJR data structure must be explicitly documented and reviewed based on the FWJR documentation. Since the latter is very sparse I suggest to improve it as of my [WMArchive issue #216](https://github.com/dmwm/WMArchive/issues/216). I list the assumptions I make in the procedure [below](#assumptions-made-in-the-aggregation-procedure).
- The algorithm to combine performance metrics from multiple steps in _phase 1_ is particularly crucial to review and also discussed [below](#assumptions-made-in-the-aggregation-procedure).

## Assumptions made in the aggregation procedure

This list must be kept up-to-date in `WMArchive.Tools.fwjr_aggregator` until explicitly documented into the FWJR data structure:

- `meta_data.ts` is a UTC timestamp.
- `task` is a list separated by `/` characters, where the first value is the job's `workflow` name and the last value is the job's `task` name.
- All `steps.site` are equal.
- The first element in `flatten(steps.errors)` is the reason of failure for the job.
- All `flatten(steps.outputs.acquisitionEra)` are equal.
- All `steps.performance` combine to a job's `performance` as follows:
  - _Sum_ values with the same key.

## Running the aggregation procedure

- There are currently two parallel scripts in place that implement the aggregation procedure:
  - `WMArchive.PySpark.RecordAggregator` is old but functional, but can run into memory issues when the amount of aggregated data exceeds the worker node's capacity.

    Since this script is to be superseded by the more efficient script below, it takes a very simplistic placeholder approach to the _phase 1_ mentioned above, simply choosing the first `cmsRun` step to provide all the performane metrics and discarding the other `cmsRun` steps. It also currently exposes no option to aggregate with a precision other than _daily_, although the logic for various precisions is implemented.

    All current sample data in the MongoDB collection `vocms013:8230.aggregated.performance` was produced using this script through the wrapper mentioned below or directly calling:

    ```
    WMArchive/bin/myspark --hdir=hdfs:///cms/wmarchive/avro/YYYY/MM/DD --schema=hdfs:///cms/wmarchive/avro/schemas/current.avsc.20160914 --script=WMArchive/src/python/WMArchive/PySpark/RecordAggregator.py
    ```

    Note that the avro schema changed on September 14, 2016 and may need to be chosen differently here depending on the data to aggregate.
  - `WMArchive.Tools.fwjr_aggregator` is the far more efficient PySpark approach that will replace the script mentioned above when fully functional.

    This script can handle hourly, daily, weekly and monthly precisions. It also accepts `min_date` and `max_date` bounds that filter the FWJRs included in the aggregation. Since FWJR timestamps often do not precisely correspond to the date of the HDFS folder they reside in, it is advisable to specify the aggregation over the entire HDFS data and use these date bounds for the filtering.

    I implemented most of the aggregation procedure in [WMArchive.Tools.fwjr_aggregator](https://github.com/knly/WMArchive/blob/master/src/python/WMArchive/Tools/fwjr_aggregator.py) and documented the remaining tasks therein.
- Both scripts are wrapped by `WMArchive/bin/aggregate_performance`.

  This script provides functionality to select the HDFS path based on a flexible date input. You can either specify a date formatted as `YYYY-mm-dd`, a number of days ago such as `0` for today and `1` for yesterday, or a full HDFS path. You toggle between the two aggregation scripts with the flag `--use_myspark` to use the old script:

  ```
  # Aggregate yesterday's data with daily precision using the old aggregation script
  WMArchive/bin/aggregate_performance 1 --precision=day --use_myspark
  ```

  Note that the script needs adjustment for dates earlier than September 14, 2016 when the avro schema changed as mentioned above.

  I suggest to merge the functionality this script provides into `WMArchive.Tools.fwjr_aggregator` as soon as the old aggregation script is no longer needed.

## Suggested schedule

- When the `WMArchive.Tools.fwjr_aggregator` script is complete, the aggregation procedure can be scheduled on the production nodes. To this end I suggest the following schedule:
  - Run `WMArchive.Tools.fwjr_aggregator` daily with hourly precision over the previous day's data after the migration of the new FWJRs to the HDFS is complete, so possibly at 3am daily:

    ```
    # Aggregate yesterday's data with daily precision, e.g. daily at 3am
    WMArchive/bin/aggregate_performance 1 --precision=hour
    ```
  - Run `WMArchive.Tools.clean_performance` daily at any time.

    ```
    # Remove older high-precision data and regenerate it with lower precision, e.g. daily at 3am
    WMArchive/bin/clean_performance
    ```
