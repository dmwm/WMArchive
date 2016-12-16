#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : FWJRAggregator.py
Author     : Luca Menichetti <luca dot menichetti AT cern dot ch>
             Valentin Kuznetsov <vkuznet AT gmail dot com>
             Nils Leif Fischer <nfischerol AT gmail dot com>
Description:

    Replaces `WMArchive.PySpark.RecordAggregator` for aggregating performance data
    as documented in https://github.com/knly/WMArchiveAggregation.

    ## Assumptions made in the aggregation procedure

    - `meta_data.ts` is a UTC timestamp.
    - `task` is a list separated by `/` characters, where the first value is the job's `workflow` name and the last value is the job's `task` name.
    - All `steps.site` are equal.
    - The first element in `flatten(steps.errors)` is the reason of failure for the job.
    - All `flatten(steps.outputs.acquisitionEra)` are equal.
    - All `steps.performance` combine to a job's `performance` as follows:
      - _Sum_ values with the same key.

"""

# system modules
import os
import sys
import time
import json
import argparse
import time
import datetime

# spark modules
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark import StorageLevel

# pymongo modules
from pymongo import MongoClient
from bson import json_util

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def parse_date(s):
    return datetime.datetime.strptime(s, DATE_FORMAT)


class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--hdir", required=True, \
                help="HDFS directory, e.g. /cms/wmarchive/avro/2016/08")
        self.parser.add_argument("--cond", default="{}", \
                help="Condition dictionary (JSON)")
        self.parser.add_argument('--min_date', type=parse_date, \
                help="The earliest date of FWJRs to aggregate.")
        self.parser.add_argument('--max_date', type=parse_date, \
                help="The latest date of FWJRs to aggregate.")
        self.parser.add_argument('--precision', '-p', \
                choices=[ 'hour', 'day', 'week', 'month' ], required=True, \
                help="The temporal precision of aggregation.")
        muri = 'mongodb://localhost:8230'
        self.parser.add_argument('--mongo', type=str, \
                dest="muri", default=muri, \
                help="MongoDB URI, default=%s" % muri)
        dbname = 'aggregated.performance'
        self.parser.add_argument('--dbname', type=str, \
                dest="dbname", default=dbname, \
                help="MongoDB name, default=%s" % dbname)
        fout = ''
        self.parser.add_argument('--fout', type=str, \
                dest="fout", default=fout, \
                help="Output file with aggregation statistics, default=%s" % fout)

def unpack_struct(colname, df):
    "Unpack FWJR structure"
    parent = filter(lambda field: field.name == colname, df.schema.fields).pop()
    fields = parent.dataType.fields if isinstance(parent.dataType, StructType) else []
    return map(lambda x : col(colname+"."+x.name), fields)

def make_filters(fdf, cond):
    "Create series of filters based on given condition"
    # here is an example what we should achieve
    # steps = fdf.filter(fdf["meta_data.jobtype"]=="Processing")
    for key, val in cond.items():
        fdf = fdf.filter(fdf[key]==val)
    return fdf

def aggregate(hdir, cond, precision, min_date, max_date):
    "Collect aggregated statistics from HDFS"

    start_time = time.time()
    print("Aggregating {} FWJR performance data in {} matching {} from {} to {}...".format(precision.replace('y', 'i') + 'ly', hdir, cond, min_date, max_date))

    conf = SparkConf().setAppName("wmarchive fwjr aggregator")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)

    # To test the procedure in an interactive pyspark shell:
    #
    # 1. Open a pyspark shell with appropriate configuration with:
    #
    # ```
    # pyspark --packages com.databricks:spark-avro_2.10:2.0.1 --driver-class-path=/usr/lib/hive/lib/* --driver-java-options=-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*
    # ```

    # 2. Paste this:
    #
    # >>>
    # from pyspark.sql.functions import *
    # from pyspark.sql.types import *
    # hdir = '/cms/wmarchive/avro/2016/06/28*'
    # precision = 'day'
    fwjr_df = sqlContext.read.format("com.databricks.spark.avro").load(hdir)
    # <<<

    # Here we process the filters given by `cond`.
    # TODO: Filter by min_date and max_date and possibly just remove the `hdir` option and instead process the entire dataset, or make it optional.
    fwjr_df = make_filters(fwjr_df, cond)

    # 3. Paste this:
    #
    # >>>

    # Select the data we are interested in
    jobs = fwjr_df.select(
        fwjr_df['meta_data.ts'].alias('timestamp'),
        fwjr_df['meta_data.jobstate'],
        fwjr_df['meta_data.host'],
        fwjr_df['meta_data.jobtype'],
        fwjr_df['task'],
        fwjr_df['steps.site'].getItem(0).alias('site'), # TODO: improve
        fwjr_df['steps'], # TODO: `explode` here, see below
        # TODO: also select `meta_data.fwjr_id`
    )

    # Transfrom each record to the data we then want to group by:

    # Transform timestamp to start_date and end_date with given precision,
    # thus producing many jobs that have the same start_date and end_date.
    # These will later be grouped by.
    timestamp = jobs['timestamp']
    if precision == "hour":
        start_date = floor(timestamp / 3600) * 3600
        end_date = start_date + 3600
    elif precision == "day":
        start_date = floor(timestamp / 86400) * 86400
        end_date = start_date + 86400
    elif precision == "week":
        end_date = next_day(to_date(from_unixtime(timestamp)), 'Mon')
        start_date = date_sub(end_date, 7)
        start_date = to_utc_timestamp(start_date, 'UTC')
        end_date = to_utc_timestamp(end_date, 'UTC')
    elif precision == "month":
        start_date = trunc(to_date(from_unixtime(timestamp)), 'month')
        end_date = date_add(last_day(start_date), 1)
        start_date = to_utc_timestamp(start_date, 'UTC')
        end_date = to_utc_timestamp(end_date, 'UTC')

    jobs = jobs.withColumn('start_date', start_date)
    jobs = jobs.withColumn('end_date', end_date)
    jobs = jobs.withColumn('timeframe_precision', lit(precision))
    jobs = jobs.drop('timestamp')

    # Transform `task` to task and workflow name
    jobs = jobs.withColumn('taskname_components', split(jobs['task'], '/'))
    jobs = jobs.withColumn('workflow', jobs['taskname_components'].getItem(1))
    jobs = jobs.withColumn('task', jobs['taskname_components'].getItem(size(jobs['taskname_components'])))
    jobs = jobs.drop('taskname_components')

    # Extract exit code and acquisition era
    stepScopeStruct = StructType([
        StructField('exitCode', StringType(), True),
        StructField('exitStep', StringType(), True),
        StructField('acquisitionEra', StringType(), True),
    ])
    def extract_step_scope(step_names, step_errors, step_outputs):
        # TODO: improve this rather crude implementation
        exitCode = None
        exitStep = None
        for (i, errors) in enumerate(step_errors):
            if len(errors) > 0:
                exitCode = errors[0].exitCode
                exitStep = step_names[i]
                break
        acquisitionEra = None
        for outputs in step_outputs:
            if len(outputs) > 0:
                acquisitionEra = outputs[0].acquisitionEra
                break
        return (exitCode, exitStep, acquisitionEra)

    extract_step_scope_udf = udf(extract_step_scope, stepScopeStruct)
    jobs = jobs.withColumn('step_scope', extract_step_scope_udf('steps.name', 'steps.errors', 'steps.output'))
    jobs = jobs.select('*', 'step_scope.exitCode', 'step_scope.exitStep', 'step_scope.acquisitionEra').drop('step_scope')

    # <<<

    # You can check the schema at any time with:
    # ```
    # jobs.printSchema()
    # ```

    # TODO: Phase 1: Aggregation over steps
    #
    #       Each job has a list of `steps`, each with a `performance` dictionary.
    #       These performance dictionaries must be combined to one by summing their
    #       values, or possibly in a different way for each metric.
    #       E.g. if a job has 3 steps, where 2 of them have a `performance`
    #       dictionary with values such as `performance.cpu.TotalJobTime: 1` and
    #       `performance.cpu.TotalJobTime: 2`, then as a result the _job_ should
    #       have a `performance` dictionary with `performance.cpu.TotalJobTime: 3`.
    #
    #       All keys in the `performance` schema should be aggregated over in this fashion.
    #       The performance metrics are documented in https://github.com/knly/WMArchiveAggregation
    #       with a reference to `WMArchive/src/maps/metrics.json`.
    #
    #       To achieve this aggregation using pyspark-native functions, we should
    #       `explode` on the `steps` array and possibly even further down into
    #       `output` and/or `errors`, keeping track of the `meta_data.fwjr_id`.
    #       Then we can group by the `fwjr_id` and make use of the pyspark aggregation
    #       functions such as `pyspark.sql.functions.sum` similar to below.

    # Phase 2: Aggregation over jobs

    # Group jobs by scope
    # TODO: Explore if this is a performance bottleneck since everything
    #       is processed on one node. An approach based on a `reduce` function
    #       may be more feasable. That said, the `groupBy` is exactly
    #       the functionality we want to achieve and is pyspark-native,
    #       so I believe we should test this first and see if it really
    #       leads to any problems.
    scopes = jobs.groupBy([
        'start_date',
        'end_date',
        'timeframe_precision',
        'jobstate',
        'host',
        'jobtype',
        'site',
        'workflow',
        'task',
        'acquisitionEra',
        'exitCode',
        'exitStep',
    ])

    # Perform the aggregation over the grouped jobs
    stats = scopes.agg(*(
        [
            count('jobstate').alias('count')
        ] + [
            # TODO: Specify all aggregation keys here by reading the `performance` schema
            #       to take the average over all jobs.
            # avg(aggregation_key) for aggregation_key in aggregation_keys
        ]
    )).collect()

    # TODO: Reshape, so that the grouped-by keys are shifted into a `scope` dictionary
    #       and the aggregated performance metrics are shifted into a `performance`
    #       dictionary, to finally achieve the data structure detailed in
    #       https://github.com/knly/WMArchiveAggregation
    stats = [row.asDict() for row in stats]

    print("Aggregation finished in {} seconds.".format(time.time() - start_time))
#     print("Result of aggregation: {}".format(stats))

    return stats

def main():
    # Parse command line arguments
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()

    # Make sure to iterate over subfolders in HDFS directory
    hdir = opts.hdir
    if not hdir.endswith('*'):
        hdir += '*'

    # Load condition from file
    if os.path.isfile(opts.cond):
        cond = json.load(open(opts.cond))
    else:
        cond = json.loads(opts.cond)

    # Perform aggregation
    stats = aggregate(hdir, cond, opts.precision, opts.min_date, opts.max_date)

    if  opts.fout:
        with open(opts.fout, 'w') as outfile:
            json.dump(stats, outfile, default=json_util.default)
    else:
        mongo_client = MongoClient(opts.muri)
        dbname, collname = opts.dbname.split('.')
        mongo_collection = mongo_client[dbname][collname]
        mongo_collection.insert(stats)

if __name__ == '__main__':
    main()
