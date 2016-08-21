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

# spark modules
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default="", help="HDFS directory, e.g. /cms/wmarchive/avro/2016/08")
        self.parser.add_argument("--cond", action="store",
            dest="cond", default="", help="Condition dictionary (JSON)")

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

def aggregate(hdir, cond):
    "Collect aggregated statistics from HDFS"

    conf = SparkConf().setAppName("wmarchive fwjr aggregator")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    sqlContext = HiveContext(sc)

    fwjr_df = sqlContext.read.format("com.databricks.spark.avro").load(hdir)
    # working example
    # steps = fwjr_df.filter(fwjr_df["meta_data.jobtype"]=="Processing").select(explode(fwjr_df["steps"]))
    steps = make_filters(fwjr_df, cond).select(explode(fwjr_df["steps"]))

    unstructed_steps = steps.select(unpack_struct("col", steps))

    unstructed_steps.persist(StorageLevel.MEMORY_AND_DISK)

    cpu_struct = unstructed_steps.select("performance.cpu").select(unpack_struct("cpu", steps.select("col.performance.cpu")))
    storage_struct = unstructed_steps.select("performance.storage").select(unpack_struct("storage", unstructed_steps.select("performance.storage")))
    memory_struct = unstructed_steps.select("performance.memory").select(unpack_struct("memory", steps.select("col.performance.memory")))

    cpu = []
    storage = []
    memory = []
    cpu_attrs = ['TotalJobCPU', 'MinEventCPU', 'TotalEventCPU', 'AvgEventCPU', 'MaxEventTime', 'TotalJobTime', 'MinEventTime', 'AvgEventTime', 'MaxEventCPU']
    for key in cpu_attrs:
        rows = cpu_struct.agg(count(key),avg(key),sum(key),min(key),max(key)).collect()
        for row in rows:
            cpu.append(row.asDict())
    storage_attrs = ['writeTotalMB', 'readPercentageOps', 'readMaxMSec', 'readAveragekB', 'readTotalMB', 'readTotalSecs', 'readNumOps', 'readCachePercentageOps', 'readMBSec', 'writeTotalSecs']
    for key in storage_attrs:
        rows = storage_struct.agg(count(key),avg(key),sum(key),min(key),max(key)).collect()
        for row in rows:
            storage.append(row.asDict())
    memory_attrs = ['PeakValueRss', 'PeakValueVsize']
    for key in memory_attrs:
        rows = memory_struct.agg(count(key),avg(key),sum(key),min(key),max(key)).collect()
        for row in rows:
            memory.append(row.asDict())

    return {'cpu':cpu, 'storage':storage, 'memory':memory}

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()

    hdir = opts.hdir
    if  not hdir.endswith('*'):
        hdir += '*'
    if  os.path.isfile(opts.cond):
        cond = json.load(open(opts.cond))
    else:
        cond = json.loads(opts.cond)

    stats = aggregate(hdir, cond)
    print(stats)

if __name__ == '__main__':
    main()


