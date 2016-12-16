#!/usr/bin/env python
# Author: Nils Fischer <n.fischer@viwid.com>
# Tool to regularly clean aggregated performance data in MongoDB by gradually decreasing precision

import os
import argparse
import datetime
from pymongo import MongoClient

MIN_TIME = {
    'hour': datetime.timedelta(hours=48),
    'day': datetime.timedelta(days=7),
    'week': datetime.timedelta(weeks=4),
    'month': datetime.timedelta(weeks=6*4),
}

def get_aggregation_result(cursor_or_dict):
    """
    Fallback for pymongo<3.0
    """
    if type(cursor_or_dict) is dict:
        return cursor_or_dict['result']
    return list(cursor_or_dict)

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='clean_performance')
        self.parser.add_argument("--mongo", action="store",\
                dest="muri", default="mongodb://localhost:8230", \
                help="MongoDB URI")
        self.parser.add_argument("--interval", action="store",\
                dest="interval", default="3m", \
                help="interval for removal, support h (hour), d (day), m (month) notation, default 3m")
        self.parser.add_argument("--dry-run", action="store_true",\
                dest="dryrun", default="", \
                help="Construct query for DB but do not execute it, i.e. dry-run")

def min_date(interval):
    "Convert given interval into timestamp"
    if  interval.endswith('h'):
        return datetime.datetime.now() - datetime.timedelta(hours=int(interval[:-1]))
    if  interval.endswith('d'):
        return datetime.datetime.now() - datetime.timedelta(days=int(interval[:-1]))
    if  interval.endswith('m'):
        return datetime.datetime.now() - datetime.timedelta(weeks=5*int(interval[:-1]))
    msg = 'Unsupported interval, please use h/d/m notations to specify hour/day/month'
    raise NotImplementedError(msg)

def main():
    # Parse command line arguments
    optmgr  = OptionParser()
    args = optmgr.parser.parse_args()

    mongo_client = MongoClient(args.muri)
    performance_data = mongo_client['aggregated']['performance']

    mdate = min_date(args.interval)
    print("Removing all data earlier than {}...".format(mdate))
    spec = {'scope.end_date': {'$lte': mdate}}
    if  args.dryrun:
        print "MongoDB query: {}".format(spec)
    else:
        res = performance_data.remove(spec)

if __name__ == '__main__':
    main()
