"""
This is example how to write simple aggregator mapper and reducer functions for
WMArchive/Tools/myspark.py tool. It collects information about cpu/time/read/write
sizes of successfull FWJR jobs. Information is structured by agent host/site.
"""

import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util
import time
from string import digits


def get_scope_hash(scope):
    """
    Hashes the scope dictionary to provide a key for the mapper and reducer.
    """
    return str(hash(frozenset(scope.items())))


def extract_stats(record, timeframe_precision="day"):

    meta_data = record['meta_data']

    timestamp = datetime.fromtimestamp(meta_data['ts'])
    if timeframe_precision == "month":
        start_date = datetime(year=timestamp.year, month=timestamp.month)
        end_date = start_date + timedelta(months=1) # FIXME: this always increments by the same amount and does not take varying month lengths into account
    elif timeframe_precision == "week":
        start_date = datetime(year=timestamp.year, month=timestamp.month, day=timestamp.day) - timedelta(days=timestamp.weekday())
        end_date = start_date + timedelta(days=7)
    elif timeframe_precision == "day":
        start_date = datetime(year=timestamp.year, month=timestamp.month, day=timestamp.day)
        end_date = start_date + timedelta(days=1)
    elif timeframe_precision == "hour":
        start_date = datetime(year=timestamp.year, month=timestamp.month, day=timestamp.day, hour=timestamp.hour)
        end_date = start_date + timedelta(hours=1)

    taskname_components = record['task'].split('/')

    site = None
    acquisitionEra = None
    exitCode = None
    exitStep = None
    for step in record['steps']:
        if site is None:
            site = step.get('site')
        if acquisitionEra is None:
            for output in step['output']:
                acquisitionEra = output.get('acquisitionEra')
                if acquisitionEra is not None:
                    break
        if exitCode is None:
            for error in step['errors']:
                exitCode = str(error.get('exitCode'))
                exitStep = step['name']
                if exitCode:
                    break
        if site is not None and acquisitionEra is not None and exitCode is not None:
            break

    stats = { 'scope': {
        'start_date': start_date,
        'end_date': end_date,
        'timeframe_precision': timeframe_precision,
        'workflow': taskname_components[1],
        'task': taskname_components[-1],
        'host': meta_data['host'],
        'site': site,
        'jobtype': meta_data['jobtype'],
        'jobstate': meta_data['jobstate'],
        'acquisitionEra': acquisitionEra,
        'exitCode': exitCode,
        'exitStep': exitStep,
    } }

    stats['count'] = 1

    events = sum(map(lambda output: output.get('events', 0) or 0, step['output']))
    if events == 0:
        events = None
    stats['events'] = events

    def aggregate_steps_performance(steps):
        for step in steps:
            # TODO: sum metrics of cmsRun steps
            if step['name'].startswith('cmsRun'):
                performance = step['performance']
                return performance
        return {}

    stats['performance'] = aggregate_steps_performance(record['steps'])

    return stats


def aggregate_stats(stats, existing):
    if existing is None:
        return stats

    stats['count'] += existing['count']

    def get_nested(target, path):
        nested = target
        for key in path:
            if nested is None:
                return None
            nested = nested.get(key)
        return nested

    def set_nested(target, path, value):
        nested = target
        for key in path[:-1]:
            nested = nested.setdefault(key, {})
        nested[path[-1]] = value

    def aggregate_average(path):
        path_N = list(path)
        path_N[-1] += "_N"
        value = get_nested(stats, path)
        N = get_nested(stats, path_N) or 1
        existing_value = get_nested(existing, path)
        existing_N = get_nested(existing, path_N) or 1
        if value is None:
            value = existing_value
            N = existing_N
        elif existing_value is not None:
            value = (value * N + existing_value * existing_N) / (N + existing_N)
            N = N + existing_N
        set_nested(stats, path, value)
        set_nested(stats, path_N, N)

    def get_paths(d, dd, key):
        if type(d) is dict or type(dd) is dict:
            paths = []
            for subkey in set(map(lambda k: k.replace('_N', ''), (d or {}).keys()) + map(lambda k: k.replace('_N', ''), (dd or {}).keys())):
                paths += map(lambda path: [ key ] + path, get_paths(d.get(subkey), dd.get(subkey), subkey))
            return paths
        else:
            return [ [ key ] ]

    for path in get_paths(stats['performance'], existing['performance'], 'performance'):
        aggregate_average(path)

    aggregate_average([ 'events' ])

    return stats


class MapReduce(object):
    def __init__(self, spec=None):
        # spec here is redundant since our mapper and reducer does not use it
        self.spec = spec
        # Timing
        print("Starting FWJR aggregation...")
        self.start_time = time.time()

    def mapper(self, records):
        """
        Function to extract necessary information from records during spark
        collect process. It will be called by RDD.collect() object within spark.
        """
        docs = []
        for rec in records:
            if  not rec:
                continue
            docs.append(rec)
        return docs

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        stats = {}
        for items in records:
            for record in items:
                if  not record:
                    continue
                # Extract list of stats from record, generally one per step
                rstats = extract_stats(record)

                # Merge into document
                scope_hash = get_scope_hash(rstats['scope'])
                stats[scope_hash] = aggregate_stats(rstats, existing=stats.get(scope_hash))

        # aggregate among all stats documents
        for scope_hash, rstats in stats.items():
            stats[scope_hash] = aggregate_stats(rstats, existing=stats.get(scope_hash))

        # Remove the scope hashes and only store a list of metrics, each with their `scope` attribute.
        # This way we can store the data in MongoDB and later filter/aggregate using the `scope`.
        stats = stats.values()
        print("### total number of collected stats", len(stats))

        # Also dump results to json file
        with open('RecordAggregator_result.json', 'w') as outfile:
            # json.dump(document, outfile, default=json_util.default)
            json.dump(stats, outfile, default=json_util.default)

        if  len(stats):
            # Store in MongoDB
            mongo_client = MongoClient('mongodb://localhost:8230') # TODO: read from config
            mongo_collection = mongo_client['aggregated']['performance']
            mongo_collection.insert(stats)
            print("Aggregated performance metrics stored in MongoDB database {}.".format(mongo_collection))

        print("--- {} seconds ---".format(time.time() - self.start_time))

        return stats
