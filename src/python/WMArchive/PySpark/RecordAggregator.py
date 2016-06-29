"""
This is example how to write simple aggregator mapper and reducer functions for
WMArchive/Tools/myspark.py tool. It collects information about cpu/time/read/write
sizes of successfull FWJR jobs. Information is structured by agent host/site.
"""

import json
from datetime import datetime
from pymongo import MongoClient


def get_scope_hash(scope):
    """
    Hashes the scope dictionary to provide a key for the mapper and reducer.
    """
    return str(hash(frozenset(scope.items())))


class MapReduce(object):
    def __init__(self, spec=None):
        # spec here is redundant since our mapper and reducer does not use it
        self.spec = spec

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
            # TODO: Assuming daily aggregation here, extend to arbitrary timeframes
            if not 'date' in document:
                timestamp = meta_data['ts']
                document['date'] = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
                # FIXME: This is not unique for a daily folder in HDFS: e.g. for 2016/06/28 there are 28.6. and 29.6. timestamps

            # Treat every step as a separate job(?)
            for step in record['steps']:

                # Define scope
                scope = {
                    "host": meta_data['host'],
                    "site": step['site'],
                    "jobtype": meta_data['jobtype'],
                    "jobstate": meta_data['jobstate'],
                }
                scope_hash = get_scope_hash(scope)

                # Retrieve existing stats for this scope
                stats = document['stats'].get(scope_hash)
                if not stats:
                    stats = {
                        'scope': scope,
                    }


                # Aggregate metrics

                # Count
                if not stats.get('count'):
                    stats['count'] = 0
                stats['count'] += 1

                # Performance
                if not stats.get('performance'):
                    stats['performance'] = {
                        "cpu": {
                            "TotalJobCPU": 0,
                            "TotalJobTime": 0,
                        },
                        "storage": {
                            "readTotalMB": 0,
                            "writeTotalMB": 0,
                        }
                    }
                performance_cpu = step['performance']['cpu']
                if performance_cpu.get('TotalJobCPU', 0):
                    stats['performance']['cpu']['TotalJobCPU'] += performance_cpu.get('TotalJobCPU', 0)
                if performance_cpu.get('TotalJobTime', 0):
                    stats['performance']['cpu']['TotalJobTime'] += performance_cpu.get('TotalJobTime', 0)
                performance_storage = step['performance']['storage']
                if performance_storage.get('readTotalMB', 0):
                    stats['performance']['storage']['readTotalMB'] += performance_storage.get('readTotalMB', 0)
                if performance_storage.get('writeTotalMB', 0):
                    stats['performance']['storage']['writeTotalMB'] += performance_storage.get('writeTotalMB', 0)

                # Store stats in document
                document['stats'][scope_hash] = stats

        return document

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        document = {
            'stats': {},
        }
        for existing_document in records:
            if not document.get('date'):
                document['date'] = existing_document['date']
                print("Using document date {}".format(document['date']))

            for scope_hash, existing_stats in existing_document['stats'].items():

                stats = document['stats'].get(scope_hash)
                if not stats:
                    stats = existing_stats
                else:
                    stats['count'] += existing_stats['count']
                    stats['performance']['cpu']['TotalJobCPU'] += existing_stats['performance']['cpu']['TotalJobCPU']
                    stats['performance']['cpu']['TotalJobTime'] += existing_stats['performance']['cpu']['TotalJobTime']
                    stats['performance']['storage']['readTotalMB'] += existing_stats['performance']['storage']['readTotalMB']
                    stats['performance']['storage']['writeTotalMB'] += existing_stats['performance']['storage']['writeTotalMB']

                document['stats'][scope_hash] = stats

        # Remove the scope hashes and only store a list of metrics, each with their `scope` attribute.
        # This way we can store the data in MongoDB and later filter/aggregate using the `scope`.
        document['stats'] = document['stats'].values()

        # Also dump results to json file
        with open('RecordAggregator_result.json', 'w') as outfile:
            json.dump(document, outfile)

        # Store in MongoDB
        mongo_client = MongoClient('mongodb://localhost:8230') # TODO: read from config
        daily_collection = mongo_client['performance']['daily']
        daily_collection.insert(document)

        print("Aggregated performance metrics for {} stored in MongoDB database {}.".format(document['date'], daily_collection))

        return document
