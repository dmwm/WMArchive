#-*- coding: ISO-8859-1 -*-
# Author: Valentin Kuznetsov <vkuznet AT gmail dot com>, Nils Fischer <n dot fischer AT stud dot uni-heidelberg dot de>
"""
This MapReduce module implements aggregation over FWJR records fetched from HDFS.
The information is aggregated across several metrics and structured by agent,host,site.
"""

import os
import time
import json
import hashlib
from string import digits
from datetime import datetime, timedelta

try:
    from pymongo import MongoClient
    from bson import json_util
except:
    pass

try:
    from wmarchi_config import MONGOURI
except:
    MONGOURI = 'mongodb://localhost:8230'

def get_scope_hash(scope):
    """
    Hashes the scope dictionary to provide a key for the mapper and reducer.
    """
    record = dict(scope) # make a copy
    for key in ['start_date', 'end_date']:
        if  key in record and isinstance(record[key], datetime):
            record[key] = record[key].isoformat()
    rec = json.JSONEncoder(sort_keys=True).encode(record)
    keyhash = hashlib.md5()
    try:
        keyhash.update(rec)
    except TypeError: # python3
        keyhash.update(rec.encode('ascii'))
    return keyhash.hexdigest()

def valid_exitCode(exitCode):
    "Check if exit code is valid"
    return exitCode not in (None, 'None', 99999, '99999')

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

    task = record['task']
    if '/' in task: # FWJR task name /a/b/c
        taskname_components = task.split('/')
    elif ':' in task: # CRAB task name 123:abc
        taskname_components = task.split(':')
    else: # default
        taskname_components = 'workflow-NA', 'task-NA'

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
        if  not valid_exitCode(exitCode):
            for error in step['errors']:
                exitCode = error.get('exitCode', None)
                exitStep = step['name']
                if  valid_exitCode(exitCode):
                    break
        if site is not None and acquisitionEra is not None and valid_exitCode(exitCode):
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
                if d and dd:
                    paths += map(lambda path: [ key ] + path, get_paths(d.get(subkey), dd.get(subkey), subkey))
            return paths
        else:
            return [ [ key ] ]

    for path in get_paths(stats['performance'], existing['performance'], 'performance'):
        aggregate_average(path)

    aggregate_average([ 'events' ])

    return stats

def serialize_stats(stats):
    "Serialize all parts of stats"
    for rec in stats:
        scope = rec['scope']
        sdate = scope['start_date']
        if hasattr(sdate, 'isoformat'):
            scope['start_date'] = sdate.isoformat()
        edate = scope['end_date']
        if hasattr(edate, 'isoformat'):
            scope['end_date'] = edate.isoformat()
        rec['scope'] = scope
    return stats

class MapReduce(object):
    def __init__(self, spec=None):
        self.name = __file__.split('/')[-1]
        # spec here is redundant since our mapper and reducer does not use it
        self.spec = spec
        self.mongouri = MONGOURI
        self.verbose = spec.get('verbose', False) if spec else False
        self.precision = spec.get('precision', 'day')
        if  self.verbose:
            print("Starting FWJR aggregation...")
        self.start_time = time.time()

    def mapper(self, pair):
        """
        Function to filter given pair from RDD, see myspark.py
        """
        return True

    def reducer(self, records):
        "Simpler reducer which collects all results from RDD.collect() records"
        stats = {}
        if  self.verbose:
            print("### Mapper found %s matches" % len(records))
        for record in records:
            if  not record:
                continue
            if  isinstance(record, tuple):
                record = record[0] # get record from (rec,key) pair of RDD
            # Extract list of stats from record, generally one per step
            rstats = extract_stats(record, self.precision)

            # Merge into document
            scope_hash = get_scope_hash(rstats['scope'])
            stats[scope_hash] = aggregate_stats(rstats, existing=stats.get(scope_hash))

        # Store results
        stats = stats.values()

        # Add hash to scope elements
        for rec in stats:
            rec['hash'] = get_scope_hash(rec['scope'])

        # write results to external json file if necessary
        if  self.verbose:
            print("### total number of collected stats", len(stats))
            # VK 20170117: I can't write to external file since
            # serialize_stats(stats) will change docs, in particular
            # it will change start_date/end_date from dateformat into string
#             with open('/tmp/wma_agg.json', 'w') as ostream:
#                 ostream.write(json.dumps(serialize_stats(stats)))

        if  len(stats):
            try: # store to mongoDB
                agg_db = os.environ.get('WMARCHIVE_PERF_DB', 'aggregated')
                agg_col = os.environ.get('WMARCHIVE_PERF_COLL', 'day')
                client = MongoClient(self.mongouri)
                coll = client[agg_db][agg_col]
                print("### RecordAggregator stores results to", coll)
                coll.create_index("hash")
                hashes = [d['hash'] for d in stats]
                spec = {'hash':{'$in':hashes}}
                existing_hashes = [d['hash'] for d in coll.find(spec)]
                for doc in stats:
                    if  doc['hash'] in existing_hashes:
                        continue
                    try:
                        coll.insert(doc)
                    except InvalidDocument as exp:
                        self.log('WARNING InvalidDocument: %s' % str(exp))
                    except InvalidOperation as exp:
                        self.log('WARNING InvalidOperation: %s' % str(exp))
                    except DuplicateKeyError as exp:
                        pass
                    except Exception as exp:
                        raise Exception(str(exp))
                if  self.verbose:
                    print("Aggregated performance metrics stored in MongoDB database {}.".format(coll))
            except Exception as exp:
                print("WMArchive:ERROR, fail to store results to MongoDB")
                print(str(exp))

        if  self.verbose:
            print("--- {} seconds ---".format(time.time() - self.start_time))

        return serialize_stats(stats)
