# Pending improvements

I suggest the following improvements in approximately descending priority:

**Aggregation procedure:**

- Move from the `WMArchive.PySpark.RecordAggregator` aggregation script to the more efficient `WMArchive.Tools.fwjr_aggregator` to produce the performance data stored in the `aggregated.performance` cache in MongoDB.

  Refer to the [Aggregation procedure](../docs/aggregation-procedure.md) documentation for details. **High priority.**
- Thoroughly check the last step of the aggregation pipeline, where the records stored in the `aggregated.performance` cache in MongoDB are aggregated given the user query in the `WMArchive.Storage.MongoIO.MongoStorage.performance` endpoint to produce the output that is then visualized in the UI.

  In particular, make sure that the performance metric averages are calculated correctly by taking into account that the records in the cache already represent an average over a varying number of jobs.
- Schedule the aggregation procedure on production nodes as suggested in the [Aggregation procedure](../docs/aggregation-procedure.md) documentation. **High priority.**
- Improve performance of the MongoDB `aggregated.performance` collection by introducing appropriate [indices](http://docs.mongodb.com/manual/indexes).

  Find implementation details on the usage of the database in the [Performance data REST endpoint](../docs/performance-data-rest-endpoint.md) documentation and directly in `WMArchive.Storage.MongoIO.MongoStorage.performance`.
- Also use short-term FWJR buffer database to provide realtime data in the UI.

  For this, access the buffer database in the MongoDB collection `fwjr.db` in `WMArchive.Storage.MongoIO.MongoStorage.performance` and aggregate over these FWJRs in a similar way as the [Aggregation procedure](../docs/aggregation-procedure.md) handles FWJRs in the HDFS. Combine the data aquired in this fashion with the data aggregated from the performance data cache in `aggregated.performance` before returning it from the `/data/performance` endpoint. Possibly implement an animated realtime refresh mechanism in the UI when the current date is selected as `end_date`.

**UI:**

- Option to share individual visualizations and possibly save them as images
- Possibly implement extended sorting functionality
- Possibly add legends to visualizations where popover tooltips are not quite sufficient
- Improve ordering behaviour of visualization widgets, possibly including support for rearranging.
- Include error margins and more statistics in visualizations
