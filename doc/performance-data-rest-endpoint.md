# Performance data REST endpoint

## Usage

- The WMArchive performance data is exposed by the `/data/performance` endpoint, e.g.:

  [https://cmsweb.cern.ch/wmarchive/data/performance?metrics[]=jobstate&axes[]=host](https://cmsweb.cern.ch/wmarchive/data/performance?metrics[]=jobstate&axes[]=host)

  or on the test deployment:

  [https://cmsweb-testbed.cern.ch/wmarchive/data/performance?metrics[]=jobstate&axes[]=host](https://cmsweb-testbed.cern.ch/wmarchive/data/performance?metrics[]=jobstate&axes[]=host)
- Requests to this endpoint can include the following query arguments:
  - A list of `metrics` where each is a dot-separated key path into the `performance` dictionary in the [performance data structure](./performance-data-structure.md), or `jobstate` or `data.events`.
  - A list of `axes` where each is a key in the `scope` dictionary in the [performance data structure](./performance-data-structure.md).
  - A `start_date` and `end_date` formatted as `YYYYMMDD` that give the timeframe of the requested data.
  - Any number of scope filters as key-value pairs where the key follows the same requirements as `axes` and the value is a valid regular expression.
  - A list of `suggestions` with the same requirements as `axes`.
- The resulting payload is of the following structure:

  ```json
  {
    "result": [
      {
        "performance": { ... }
      }
    ]
  }
  ```

  The `performance` dictionary then contains the requested data in the following keys:
    - `status`:
      - `time` is the processing time of the aggregation
      - `totalMatchedJobs` is the number of jobs matched by the given scope filters and timeframe
      - `start_date` and `end_date` give the bounds of all matched job timestamps
      - `max_date` and `min_date` give the bounds of all job timestamps available in the database
    - `visualizations` is a nested dictionary that contains the aggregated data for each metric and axis given in `metrics` and `axes`. The data is a list of objects, each with a `label` corresponding to their axis value. For the `jobstate` metric they include a list of `jobstates` each with a `count` and `jobstate`, whereas for other metrics they include the aggregated `average` value as well as the `count` of jobs that were aggregated over.

    This data is intended to be visualized using a framework such as [D3.js](https://d3js.org) or [Chart.js](http://www.chartjs.org).
    - `suggestions` gives all distinct values in the database for each scope key given in the `suggestions` query argument list. It takes into account all scope filters and timeframe given in the query, except for the one the distinct values are collected for.

    This list is intended to serve as scope filter input suggestions.
    - Finally, `supplementaryData` contextually provides additional data. In particular it includes the complete dictionary of `exitCodes` descriptions if `exitCode` is requested either in `axes` or `suggestions`, and the complete set of `metrics` if none was requested.

    The `exitCodes` dictionary is intended to provide human-readable descriptions for exit codes and is discussed in [Report 009](../009_2016-09-02.md#error-exit-codes-in-ui).

    The `metrics` dictionary is intended to provide the display and formatting information for each metric and is discussed in [Report 011](../011_2016-09-16.md#loading-metrics-dynamically).

## Implementation

- As of the configuration in `WMArchive/etc` for the `WMCore` server backend the REST API is served from the `/data` endpoint given by `WMArchive.Service.Data.WMAData`.
- The `WMAData` class parses the request arguments. I kept the changes to a minimum by only adding the `/data/performance` endpoint. However, I suggest to improve the remainder of the REST API that I assume is not yet entirely conceptualized, to follow a more consistent structure. Functionality parallel to `/data/performance` should be exposed as distinct endpoints such as `/data/status` or even `/status` instead of query arguments.
- The `/data/performance` endpoint's query arguments are processed in `WMAData.validate`. Additional arguments such as scope filters must be added here along with their valid regular expression pattern.
- Valid requests then return the result of `WMAData.get`. I also only added the `/data/performance` endpoint here without changes to the general result data structure. I suggest to remove the complexity introduced by returning a list of dictionaries here and instead move the responsibility of defining their result data structure to the individual endpoints. For example the `/data/performance` endpoint would ideally just return `result[0].performance` directly.
- Note that query arguments that take a list appear in the request URL as multiple occurences of the argument name suffixed with `[]` such as `metrics[]`. They are validated by `validate_strlist` in `WMAData.validate` with this suffix, but it must be removed in `WMAData.get` before passing it to Python functions as arguments.
- The responsibility of retrieving the return data lies within `WMAData`'s `WMArchiveManager`. Through a chain of calls to a `performance` function, this responsiblity is passed through `WMArchive.Service.Manager.WMArchiveManager` over `WMArchive.Service.STS.STSManager` to `WMArchive.Storage.MongoIO.MongoStorage`.
- The `MongoStorage.performance` function implements the filtering and aggregation logic to collect the requested data from MongoDB.
  - The MongoDB database and collection to use are read from the environment variables `WMARCHIVE_PERF_DB` and `WMARCHIVE_PERF_COLL` and default to `aggregated.performance`.
  - The procedure is based on [MongoDB's aggregation pipeline](https://docs.mongodb.com/manual/core/aggregation-pipeline/).
  - If requested, also `exitCodes` and the complete set of `metrics` is read from the `WMARCHIVE_ERROR_CODES` and `WMARCHIVE_PERF_METRICS` environment variables. Particularly the latter must be set to provide the UI with information on the metrics to present and is discussed in [Report 011](../011_2016-09-16.md#loading-metrics-dynamically).
