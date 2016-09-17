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
"""

# system modules
import os
import sys
import time
import json
import argparse
import time
import datetime
import logging
logger = logging.getLogger(__name__)

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
        self.parser.add_argument("--hdir", required=True, help="HDFS directory, e.g. /cms/wmarchive/avro/2016/08")
        self.parser.add_argument("--cond", default="{}", help="Condition dictionary (JSON)")
        self.parser.add_argument('--min_date', type=parse_date, help="The earliest date of FWJRs to aggregate.")
        self.parser.add_argument('--max_date', type=parse_date, help="The latest date of FWJRs to aggregate.")
        self.parser.add_argument('--precision', '-p', choices=[ 'hour', 'day', 'week', 'month' ], required=True, help="The temporal precision of aggregation.")

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
    logger.info("Aggregating {} FWJR performance data in {} matching {} from {} to {}...".format(precision.replace('y', 'i') + 'ly', hdir, cond, min_date, max_date))

    conf = SparkConf().setAppName("wmarchive fwjr aggregator")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)

    # ---
    # 1. Open a pyspark shell with appropriate configuration with:
    # ```
    # pyspark --packages com.databricks:spark-avro_2.10:2.0.1 --driver-class-path=/usr/lib/hive/lib/* --driver-java-options=-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*
    # ```
    # 2. Paste this:
    # from pyspark.sql.functions import *
    # from pyspark.sql.types import *
    # hdir = '/cms/wmarchive/avro/2016/06/28*'
    # precision = 'day'
    fwjr_df = sqlContext.read.format("com.databricks.spark.avro").load(hdir)
    # ---

    fwjr_df = make_filters(fwjr_df, cond)

    # ---
    # 3. Paste this:
    jobs = fwjr_df.select(
        fwjr_df['meta_data.ts'].alias('timestamp'),
        fwjr_df['meta_data.jobstate'],
        fwjr_df['meta_data.host'],
        fwjr_df['meta_data.jobtype'],
        fwjr_df['task'],
        fwjr_df['steps.site'].getItem(0).alias('site'),
        fwjr_df['steps'],
    )

    # Timeframe
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

    # Task and workflow
    jobs = jobs.withColumn('taskname_components', split(jobs['task'], '/'))
    jobs = jobs.withColumn('workflow', jobs['taskname_components'].getItem(1))
    jobs = jobs.withColumn('task', jobs['taskname_components'].getItem(size(jobs['taskname_components'])))
    jobs = jobs.drop('taskname_components')

    # Exit code and acquisition era
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

    # jobs.printSchema()
    # ---


    # Aggregation over steps
    # TODO: Each job has an array of `steps`, each with a `performance` dictionary.
    #       These performance dictionaries must be combined to one by summing their
    #       values.
    #       E.g. if a job has 3 steps, where 2 of them have a `performance`
    #       dictionary with values such as `performance.cpu.TotalJobTime: 1` and
    #       `performance.cpu.TotalJobTime: 2`, then as a result the _job_ should
    #       have a `performance` dictionary with `performance.cpu.TotalJobTime: 3`.
    #
    #       All keys in the `performance` schema should be aggregated over in this fashion.


    # Group jobs by scope
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


    # Aggregation over jobs
    stats = scopes.agg(*(
        [
            count('jobstate').alias('count')
        ] + [
            # avg(aggregation_key) for aggregation_key in aggregation_keys
            # TODO: Specify all aggregation keys from the `performance` schema here
            #       to take the average over all jobs.
        ]
    )).collect()

    # TODO: Reshape, so that the grouped-by keys are shifted into a `scope` dictionary
    #       and the aggregated performance metrics are shifted into a `performance`
    #       dictionary.
    stats = [row.asDict() for row in stats]

    logger.info("Aggregation finished in {} seconds.".format(time.time() - start_time))
    logger.debug("Result of aggregation: {}".format(stats))

    return stats

def main():
    logging.basicConfig(level=logging.WARNING)
    logger.setLevel(logging.DEBUG)

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

    # Dump results to json file
    with open('aggregated_performance_data.json', 'w') as outfile:
        json.dump(stats, outfile, default=json_util.default)
        logger.info("Written result to {}.".format(outfile))

    # Store in MongoDB
    # mongo_client = MongoClient('mongodb://localhost:8230') # TODO: read from config
    # mongo_collection = mongo_client['aggregated']['performance']
    # mongo_collection.insert(stats)
    # logger.info("Stored in MongoDB collection {}.".format(mongo_collection))

if __name__ == '__main__':
    main()
