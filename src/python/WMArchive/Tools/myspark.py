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
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default="", help="Input data location on HDFS, e.g. hdfs:///path/data")
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="", help="Input schema, e.g. hdfs:///path/fwjr.avsc")
        self.parser.add_argument("--mapper", action="store",
            dest="mapper", default="", help="User based mapper python script, should contain extract function")
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def extract(records):
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

def run(schema_file, data_path, mapper=None, verbose=None):
    # pyspark modules
    from pyspark import SparkContext

    # define spark context, it's main object which allow
    # to communicate with spark
    sc = SparkContext(appName="AvroKeyInputFormat")

    # load FWJR schema
    rdd = sc.textFile(schema_file, 1).collect()

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
    avro_rdd = sc.newAPIHadoopFile(data_path, aformat, akey, awrite, aconv, conf=conf)

    # process data, here the map will read record from avro file
    # if we need a whole record we'll use lambda x: x[0], e.g.
    # output = avro_rdd.map(lambda x: x[0]).collect()
    #
    # if we need a particular key, e.g. jobid, we'll extract it
    # within lambda function, e.g. lambda x: x[0]['jobid'], e.g.
    # output = avro_rdd.map(lambda x: x[0]['jobid']).collect()
    #
    # in more general way we write extract function which will be
    # executed by Spark via collect call
    if  mapper:
        from mapper import extract as user_extract
        records = avro_rdd.map(user_extract).collect()
    else:
        records = avro_rdd.map(extract).collect()
    sc.stop()
    return records

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    results = run(opts.schema, opts.hdir, opts.mapper, opts.verbose)
    if  opts.verbose:
        print("RESULTS", results)

if __name__ == '__main__':
    main()

