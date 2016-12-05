#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Skeleton.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Skeleton MR python. It defines how to read the data
             from HDFS via read function as well as provides examples
             of simple Map and Reduce classes.
"""

# system modules
import os
import sys
import io
import gzip

# pydoop modules
try:
    import pydoop.mapreduce.api as api
    import pydoop.mapreduce.pipes as pp
    import pydoop.hdfs as hdfs
except: # wrap for sphinx
    pass

# avro modules
import avro.schema
import avro.io

def read_avro(fname, schema):
    "Internal API to read data from HDFS files"
    out = []
    data = hdfs.load(fname)
    bytes_reader = io.BytesIO(data)

    if  fname.endswith('.gz'):
        # use gzip'ed reader and pass to it BytesIO as file object
        gzip_reader = gzip.GzipFile(fileobj=bytes_reader)
        decoder = avro.io.BinaryDecoder(gzip_reader)
    else:
        # use non-compressed reader
        decoder = avro.io.BinaryDecoder(bytes_reader)

    reader = avro.io.DatumReader(schema)
    while True:
        try:
            rec = reader.read(decoder)
            out.append(rec)
        except:
            break
    # close gzip stream if necessary
    if  fname.endswith('.gz'):
        gzip_reader.close()

    # close bytes stream
    bytes_reader.close()
    return out

class Reader(api.RecordReader):
    """Custom Reader class. It reads data from HDFS and extract records for MR job"""
    def __init__(self, context):
        super(Reader, self).__init__(context)

        fid = context.input_split.filename

        self.read_schema(context)
        self.data = self.read(fid)
        self.idx = 0 # first element

    def read_schema(self, context):
        sfile = context.get_job_conf().get('avro.schema', None)
        try:
            schemaData = hdfs.load(sfile)
        except ValueError:
            # if not sfile:
            # else:
            sys.exit(1)

        self.schema = avro.schema.parse(schemaData)

    def read(self, fname):
        return read_avro(fname, self.schema)

    def next(self):
        "Read next record from data stream and return key, value pairs for Mapper"
        if  self.idx >= len(self.data):
            raise StopIteration
        rec = self.data[self.idx]
        self.idx += 1
        key = rec['jobid']
        return key, rec

    def get_progress(self):
        "Progress function implementation"
        return float(self.idx)/len(self.data)

class Mapper(api.Mapper):
    """Example of Mapper class"""
    def __init__(self, context):
        super(Mapper, self).__init__(context)

    def map(self, context):
        "Read given context and yield key (job-id) and values (task)"
        mapper(context) # user defined mapper function for given context

class Reducer(api.Reducer):
    """Example of Reducer class"""
    def __init__(self, context):
        super(Reducer, self).__init__(context)

    def reduce(self, context):
        "Emit empty key and some data structure via given context"
        reducer(context) # user defined reducer function for given context

def __main__():
    """Main function to be executed by pydoop framework"""
    factory = pp.Factory(mapper_class=Mapper, reducer_class=Reducer, record_reader_class=Reader)
    pp.run_task(factory, private_encoding=True)
