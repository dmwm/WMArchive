# Common Tasks

## Adding scope filters

- Adjust `WMArchive.Service.Data.WMAData.validate` to validate the additional query argument with a regular expression as detailed in [the Performance data REST endpoint documentation](docs/performance-data-rest-endpoint.md).
- Adjust `WMArchive.Storage.MongoIO.MongoStorage.performance` by adding the scope filter to the valid `scope_keys`.
- Adjust `WMArchive/src/js/models/scope.js` by adding the scope filter to `app.Scope.filters` and a default value to `app.Scope.defaults` as detailed in [the Performance UI architecture documentation](docs/performance-ui-architecture.md).

## Changing metrics

- Check the value of the `WMARCHIVE_PERF_METRICS` environment variable at runtime of the server. It may point to `WMArchive/src/maps/metrics.json`.
- Edit the file with your changes. Refer to [Report 011](011_2016-09-16.md#loading-metrics-dynamically) for details and **make sure to test the server with your changes since the UI relies on this information**.
