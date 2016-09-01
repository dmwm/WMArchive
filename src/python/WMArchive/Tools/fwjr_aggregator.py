#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : FWJRAggregator.py
Author     : Luca Menichetti <luca dot menichetti AT cern dot ch>
             Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
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

    fwjr_df = sqlContext.read.format("com.databricks.spark.avro").load(hdir)

    jobs = make_filters(fwjr_df, cond)
    steps = jobs.select('meta_data', 'task', explode(fwjr_df['steps']).alias('step'))
    stats = steps.groupBy([
        # TODO: start_date
        # TODO: end_date
        # TODO: timeframe_precision (just set value)
        'meta_data.jobstate',
        'meta_data.host',
        'meta_data.jobtype',
        'step.name',
        'step.site',
        split(steps['task'], '/').getItem(1).alias('workflow'),
        # TODO: split(steps['task'], '/').getItem(-1).alias('task'),
        # TODO: acquisitionEra (step.output(array).acquisitionEra(any/first))
        # TODO: exitCode (step.errors(array).exitCode(any/first))
    ]).agg(
        avg('step.performance.cpu.TotalJobTime')
    ).collect()

    # TODO: reshape to performance data structure
    stats = [row.asDict() for row in stats]

    logger.info("Aggregation finished in {} seconds.".format(time.time() - start_time))
    logger.debug("Result of aggregation: {}".format(stats))

    return stats

def main():
    logging.basicConfig(level=logging.WARNING)
    logger.setLevel(logging.DEBUG)

    # Parse command line arguments
    optmgr  = OptionParser()
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
