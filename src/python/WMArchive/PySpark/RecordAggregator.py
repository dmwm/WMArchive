"""
This is example how to write simple aggregator mapper and reducer functions for
WMArchive/Tools/myspark.py tool. It collects information about cpu/time/read/write
sizes of successfull FWJR jobs. Information is structured by agent host/site.
"""

import json
from datetime import datetime
from pymongo import MongoClient
from bson import json_util
import time
from string import digits


def get_scope_hash(scope):
    """
    Hashes the scope dictionary to provide a key for the mapper and reducer.
    """
    return str(hash(frozenset(scope.items())))


def extract_stats(record):

    meta_data = record['meta_data']
    taskname_components = record['task'].split('/')

    def extract_stats_from_step(step):

        acquisitionEra = None
        for output in step['output']:
            acquisitionEra = output.get('acquisitionEra')
            if acquisitionEra is not None:
                break
        exitCode = None
        for error in step['errors']:
            exitCode = error.get('exitCode')
            if exitCode is not None:
                break

        stats = { 'scope': {
            'workflow': taskname_components[1],
            'task': taskname_components[-1],
            'host': meta_data['host'],
            'site': step['site'],
            'jobtype': meta_data['jobtype'],
            'jobstate': meta_data['jobstate'],
            'step': step['name'].rstrip(digits),
            'acquisitionEra': acquisitionEra,
            'exitCode': exitCode,
        } }

        stats['count'] = 1

        stats['performance'] = step['performance']

        events = sum(map(lambda output: output.get('events', 0) or 0, step['output']))
        if events == 0:
            events = None
        stats['events'] = events

        return stats

    return map(extract_stats_from_step, record['steps'])


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
        document = {
            'stats': {}
        }
        for record in records:
            if not record:
                # FIXME: This happens many times
                continue

            meta_data = record['meta_data']

            # Determine timeframe of aggregation
            timestamp = datetime.fromtimestamp(meta_data['ts'])
            if not 'start_date' in document or timestamp < document['start_date']:
                document['start_date'] = timestamp
            if not 'end_date' in document or timestamp > document['end_date']:
                document['end_date'] = timestamp

            # Extract list of stats from record, generally one per step
            stats_list = extract_stats(record)

            # Merge into document
            for stats in stats_list:
                scope_hash = get_scope_hash(stats['scope'])
                document['stats'][scope_hash] = aggregate_stats(stats, existing=document['stats'].get(scope_hash))

        return document


    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        document = {
            'stats': {},
        }
        for existing_document in records:
            if not 'start_date' in document or existing_document['start_date'] < document['start_date']:
                document['start_date'] = existing_document['start_date']
            if not 'end_date' in document or existing_document['end_date'] > document['end_date']:
                document['end_date'] = existing_document['end_date']

            for scope_hash, existing_stats in existing_document['stats'].items():
                document['stats'][scope_hash] = aggregate_stats(existing_stats, existing=document['stats'].get(scope_hash))


        # Remove the scope hashes and only store a list of metrics, each with their `scope` attribute.
        # This way we can store the data in MongoDB and later filter/aggregate using the `scope`.
        stats = document['stats'].values()
        for stat in stats:
            stat['start_date'] = document['start_date']
            stat['end_date'] = document['end_date']

        # Also dump results to json file
        with open('RecordAggregator_result.json', 'w') as outfile:
            # json.dump(document, outfile, default=json_util.default)
            json.dump(stats, outfile, default=json_util.default)

        # Store in MongoDB
        mongo_client = MongoClient('mongodb://localhost:8230') # TODO: read from config
        daily_collection = mongo_client['performance']['daily']
        daily_collection.insert(stats)

        print("Aggregated performance metrics stored in MongoDB database {}.".format(daily_collection))
        print("--- {} seconds ---".format(time.time() - self.start_time))

        return document
