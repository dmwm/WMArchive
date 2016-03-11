#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : myspark.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Example file to run basic spark job via pyspark

This code is based on example provided at
https://github.com/apache/spark/blob/master/examples/src/main/python/avro_inputformat.py

PySpark APIs:
https://spark.apache.org/docs/0.9.0/api/pyspark/index.html
"""

# system modules
import os
import sys
import imp
import time
import json
import argparse

# WMArchive modules
from WMArchive.Utils.Utils import htime

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        msg = "Input data location on HDFS, e.g. hdfs:///path/data"
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default="", help=msg)
        msg = "Input schema, e.g. hdfs:///path/fwjr.avsc"
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="", help=msg)
        msg = "python script with custom mapper/reducer functions"
        self.parser.add_argument("--script", action="store",
            dest="script", default="", help=msg)
        msg = "json file with query spec"
        self.parser.add_argument("--spec", action="store",
            dest="spec", default="", help=msg)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def basic_mapper(records):
    """
    Function to extract necessary information from record during spark
    collect process. It will be called by RDD.collect() object within spark.
    """
    out = []
    for rec in records:
        # extract jobid from existing records
        # we must always check if iterable rec object is a real dict, i.e.
        # data extracted from our file
        if  rec and isinstance(rec, dict):
            out.append(rec['jobid'])
    return out

def basic_reducer(records):
    """
    Basic reducer implementation. It shows that if our mapper yield
    a list of records we should handle them correctly via internal loop.
    """
    out = []
    counter = 0
    for item in records:
        if  isinstance(rec, list):
            for rec in item:
                counter += 1
        else:
            counter += 1
    out.append({'ndocs': counter})
    return out

class SparkLogger(object):
    "Control Spark Logger"
    def __init__(self, ctx):
        self.logger = ctx._jvm.org.apache.log4j
        self.rlogger = self.logger.LogManager.getRootLogger()

    def set_level(self, level):
        "Set Spark Logger level"
        self.rlogger.setLevel(getattr(self.logger.Level, level))

    def lprint(self, stream, msg):
        "Print message via Spark Logger to given stream"
        getattr(self.rlogger, stream)(msg)

    def info(self, msg):
        "Print message via Spark Logger to info stream"
        self.lprint('info', msg)

    def error(self, msg):
        "Print message via Spark Logger to error stream"
        self.lprint('error', msg)

    def warning(self, msg):
        "Print message via Spark Logger to warning stream"
        self.lprint('warning', msg)

def import_(filename):
    "Import given filename"
    path, name = os.path.split(filename)
    name, ext = os.path.splitext(name)
    ifile, filename, data = imp.find_module(name, [path])
    return imp.load_module(name, ifile, filename, data)

def run(schema_file, data_path, script=None, spec_file=None, verbose=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    time0 = time.time()
    # pyspark modules
    from pyspark import SparkContext

    # define spark context, it's main object which allow
    # to communicate with spark
    ctx = SparkContext(appName="AvroKeyInputFormat")
    logger = SparkLogger(ctx)
    if  not verbose:
        logger.set_level('ERROR')

    # load FWJR schema
    rdd = ctx.textFile(schema_file, 1).collect()

    # define input avro schema, the rdd is a list of lines (sc.textFile similar to readlines)
    avsc = reduce(lambda x, y: x + y, rdd) # merge all entries from rdd list
    schema = ''.join(avsc.split()) # remove spaces in avsc map
    conf = {"avro.schema.input.key": schema}

    # define newAPIHadoopFile parameters, java classes
    aformat="org.apache.avro.mapreduce.AvroKeyInputFormat"
    akey="org.apache.avro.mapred.AvroKey"
    awrite="org.apache.hadoop.io.NullWritable"
    aconv="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter"

    # load data from HDFS
    avro_rdd = ctx.newAPIHadoopFile(data_path, aformat, akey, awrite, aconv, conf=conf)

    # process data, here the map will read record from avro file
    # if we need a whole record we'll use lambda x: x[0], e.g.
    # output = avro_rdd.map(lambda x: x[0]).collect()
    #
    # if we need a particular key, e.g. jobid, we'll extract it
    # within lambda function, e.g. lambda x: x[0]['jobid'], e.g.
    # output = avro_rdd.map(lambda x: x[0]['jobid']).collect()
    #
    # in more general way we write mapper/reducer functions which will be
    # executed by Spark via collect call
    spec = None
    if  spec_file:
        spec = json.load(open(spec_file))
    if  script:
        obj = import_(script)
        logger.info("Use user-based script %s" % obj)
        if  not hasattr(obj, 'MapReduce'):
            logger.error('Unable to find MapReduce class in %s, %s' \
                    % (script, obj))
            ctx.stop()
            return
        mro = obj.MapReduce(spec)
        # example of collecting records from mapper and
        # passing all of them to reducer function
        records = avro_rdd.map(mro.mapper).collect()
        out = mro.reducer(records)

        # the map(f).reduce(f) example but it does not collect
        # intermediate records
        # out = avro_rdd.map(obj.mapper).reduce(obj.reducer).collect()
    else:
        records = avro_rdd.map(basic_mapper).collect()
        out = basic_reducer(records)
    ctx.stop()
    if  verbose:
        logger.info("Elapsed time %s" % htime(time.time()-time0))
    return out

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    results = run(opts.schema, opts.hdir, opts.script, opts.spec, opts.verbose)
    print(results)

if __name__ == '__main__':
    main()

