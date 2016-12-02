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
import pwd
import time
import json
import urllib
import urllib2
import httplib
import argparse
import datetime

# WMArchive modules
from WMArchive.Utils.Utils import htime, wmaHash, trange

HDIR = 'hdfs:///cms/wmarchive/avro'

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        year = time.strftime("%Y", time.localtime())
        hdir = HDIR
        msg = "Input data location on HDFS, e.g. %s/%s" % (hdir, year)
        self.parser.add_argument("--hdir", action="store",
            dest="hdir", default=hdir, help=msg)
        schema = 'fwjr_prod.avsc'
        msg = "Input schema, default %s/%s" % (hdir, schema)
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="%s/%s" % (hdir, schema), help=msg)
        msg = "python script with custom mapper/reducer functions"
        self.parser.add_argument("--script", action="store",
            dest="script", default="", help=msg)
        msg = "json file with query spec or valid json"
        self.parser.add_argument("--spec", action="store",
            dest="spec", default="", help=msg)
        self.parser.add_argument("--yarn", action="store_true",
            dest="yarn", default=False, help="run job on analytics cluster via yarn resource manager")
        self.parser.add_argument("--no-log4j", action="store_true",
            dest="no-log4j", default=False, help="Disable spark log4j messages")
        msg = "store results into WMArchive, provide WMArchvie url"
        self.parser.add_argument("--store", action="store",
            dest="store", default="", help=msg)
        msg = "provide wmaid for store submission"
        self.parser.add_argument("--wmaid", action="store",
            dest="wmaid", default="", help=msg)
        msg  = 'specify private key file name, default $X509_USER_PROXY'
        self.parser.add_argument("--ckey", action="store",
                               default=x509(), dest="ckey", help=msg)
        msg  = 'specify private certificate file name, default $X509_USER_PROXY'
        self.parser.add_argument("--cert", action="store",
                               default=x509(), dest="cert", help=msg)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="verbose output")

def x509():
    "Helper function to get x509 either from env or tmp file"
    proxy = os.environ.get('X509_USER_PROXY', '')
    if  not proxy:
        proxy = '/tmp/x509up_u%s' % pwd.getpwuid( os.getuid() ).pw_uid
        if  not os.path.isfile(proxy):
            return ''
    return proxy

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    """
    Simple HTTPS client authentication class based on provided
    key/ca information
    """
    def __init__(self, key=None, cert=None, level=0):
        if  level > 1:
            urllib2.HTTPSHandler.__init__(self, debuglevel=1)
        else:
            urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert

    def https_open(self, req):
        """Open request method"""
        #Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        return self.do_open(self.get_connection, req)

    def get_connection(self, host, timeout=300):
        """Connection method"""
        if  self.key:
            return httplib.HTTPSConnection(host, key_file=self.key,
                                                cert_file=self.cert)
        return httplib.HTTPSConnection(host)

def postdata(url, data, ckey=None, cert=None, verbose=0):
    """
    POST data into given url
    """
    headers = {'Content-type':'application/json','Accept':'application/json'}
    req = urllib2.Request(url)
    for key, val in headers.iteritems():
        req.add_header(key, val)
    if  verbose > 1:
        handler = urllib2.HTTPHandler(debuglevel=1)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    if  ckey and cert:
        handler = HTTPSClientAuthHandler(ckey, cert, verbose)
        opener  = urllib2.build_opener(handler)
        urllib2.install_opener(opener)
    data = urllib2.urlopen(req, json.dumps(data))

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

def run(schema_file, data_path, script=None, spec_file=None, verbose=None, yarn=None):
    """
    Main function to run pyspark job. It requires a schema file, an HDFS directory
    with data and optional script with mapper/reducer functions.
    """
    if  verbose:
        print("### schema", schema_file)
        print("### path", data_path)
        print("### script", script)
        print("### spec", spec_file)
    time0 = time.time()
    # pyspark modules
    from pyspark import SparkContext

    # define spark context, it's main object which allow
    # to communicate with spark
    ctx = SparkContext(appName="AvroKeyInputFormat", pyFiles=[script])
    logger = SparkLogger(ctx)
    if  not verbose:
        logger.set_level('ERROR')
    if yarn:
        logger.info("YARN client mode enabled")

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
    if  isinstance(data_path, list):
        avro_rdd = ctx.union([ctx.newAPIHadoopFile(f, aformat, akey, awrite, aconv, conf=conf) for f in data_path])
    else:
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
        if  os.path.isfile(spec_file):
            spec = json.load(open(spec_file))
        else:
            spec = json.loads(spec_file)
    if  verbose:
        print("### spec", json.dumps(spec))
    if  script:
        obj = import_(script)
        logger.info("Use user-based script %s" % obj)
        if  not hasattr(obj, 'MapReduce'):
            logger.error('Unable to find MapReduce class in %s, %s' \
                    % (script, obj))
            ctx.stop()
            return
        # we have a nested use case when one MR return WMArchive spec
        # we'll loop in that case until we get non-spec output
        count = 0
        while True:
            mro = obj.MapReduce(spec)
            # example of collecting records from mapper and
            # passing all of them to reducer function
            records = avro_rdd.filter(mro.mapper).collect()
            out = mro.reducer(records)
            if  verbose:
                print("### Loop count", count)
            if  count > 3:
                print("### WARNING, loop counter exceed its limit")
                break
            if  is_spec(out):
                spec = out
            else:
                break
            count += 1

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

def is_spec(data):
    "Check if given data is WMArchive spec"
    if  not isinstance(data, dict):
        return False
    std_keys = set(['spec', 'fields'])
    if  set(data.keys()) & std_keys == std_keys:
        return True
    return False

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    time0 = time.time()

    todate = datetime.datetime.today()
    todate = int(todate.strftime("%Y%m%d"))
    fromdate = datetime.datetime.today()-datetime.timedelta(days=1)
    fromdate = int(fromdate.strftime("%Y%m%d"))
    spec = json.load(open(opts.spec)) if opts.spec else {}
    timerange = spec.get('spec', {}).get('timerange', [fromdate, todate])

    if  opts.hdir == HDIR:
        hdir = opts.hdir.split()
        if  len(hdir) == 1:
            hdir = hdir[0]
            hdirs = []
            for tval in trange(timerange):
                if  hdir.find(tval) == -1:
                    hdirs.append(os.path.join(hdir, tval))
            hdir = hdirs
    else:
        hdir = opts.HDIR
    results = run(opts.schema, hdir, opts.script, opts.spec, opts.verbose, opts.yarn)
    if  opts.store:
        data = {"results":results,"ts":time.time(),"etime":time.time()-time0}
        if  opts.wmaid:
            data['wmaid'] = opts.wmaid
        else:
            data['wmaid'] = wmaHash(data)
        data['dtype'] = 'job'
        pdata = dict(job=data)
        postdata(opts.store, pdata, opts.ckey, opts.cert, opts.verbose)
    else:
        print(results)

if __name__ == '__main__':
    main()
