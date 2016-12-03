#!/usr/bin/env python
# Author: Nils Fischer <n.fischer@viwid.com>
# Tool to run performance aggregations over the FWJR database in HDFS.
# Documentation: https://github.com/knly/WMArchiveAggregation
# TODO: Merge into `WMArchive.Tools.fwjr_aggregator` as soon as the old
#       myspark approach is no longer needed.

import os
import json
import time
import argparse
import datetime
import subprocess

def parse_source(s):

    def path_from_day(day):
        return '{year:04d}{month:02d}{day:02d}'.format(year=day.year, month=day.month, day=day.day)

    if s == 'all':
        start_date = datetime.date(year=2016, month=6, day=1)
        end_date = datetime.date.today()
        days = [ start_date + datetime.timedelta(days=n) for n in range((end_date - start_date).days) ]
        return [ path_from_day(day) for day in days ]

    try:
        return [ path_from_day(datetime.datetime.strptime(s, '%Y-%m-%d').date()) ]
    except ValueError:
        pass
    try:
        return [ path_from_day(datetime.date.today() - datetime.timedelta(days=int(s))) ]
    except:
        pass

    return s

HDIR = 'hdfs:///cms/wmarchive/avro'

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(\
                description="Run performance aggregations over the FWJR database.")
        self.parser.add_argument('source', type=parse_source, \
                help="The FWJR data to aggregate over. Provide either a date formatted like YYYY-MM-DD, a number of days ago, e.g. 0 for today or 1 for yesterday, a path in the HDFS such as /cms/wmarchive/avro/2016/08/30 or 'all'.")
        self.parser.add_argument('--precision', '-p', \
                choices=[ 'hour', 'day', 'week', 'month' ], required=True, \
                help="The temporal precision of aggregation.")
        self.parser.add_argument('--use_myspark', action='store_true', \
                help="Use legacy myspark script.")
        schema = '%s/schemas/current.avsc' % HDIR
        self.parser.add_argument('--schema', action='store', \
                default=schema, help="WMArchive schema, default=%s" % schema)
        self.parser.add_argument('--spec', action='store', \
                default="", help="Spec file to use")
        self.parser.set_defaults(use_myspark=False)

def make_spec(day):
    "Create WMA spec"
    spec = {'spec':{'timerange':[day,day]}, 'fields':['wmaid']}
    return spec

def main():
    # Parse command line arguments
    optmgr  = OptionParser()
    args = optmgr.parser.parse_args()

    start_time = time.time()
    print("Aggregating {} performance data in {}...".format(args.precision.replace('y', 'i') + 'ly', args.source))

    if args.use_myspark:
        from WMArchive.PySpark import RecordAggregator
        aggregation_script = RecordAggregator.__file__.replace('.pyc', '.py')
        print("Using myspark aggregation script in {}.".format(aggregation_script))

        for day in args.source:
            basedir = '%s/%s' % (HDIR, day)
            files = os.popen("hadoop fs -ls %s | sed '1d;s/  */ /g' | cut -d\  -f8" % basedir).read().splitlines()
            for fname in files:
                print("myspark --hdir=%s --schema=%s --script=%s"\
                        % (fname, args.schema, aggregation_script))
                subprocess.call([ 'myspark', \
                        '--hdir=' + fname, \
                        '--schema=' + args.schema, \
                        '--script=' + aggregation_script ])
    else:
        print("Using fwjr_aggregator aggregation script.")
        for source in args.source:
            subprocess.call([ 'fwjr_aggregator', '--hdir=' + source, \
                    '--precision=' + args.precision ])

    print("Completed FWJR performance data aggregation in {} seconds.".format(time.time() - start_time))

if __name__ == '__main__':
    main()
