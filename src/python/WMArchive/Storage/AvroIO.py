#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : AvroIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Avro storage module based on apache Avro python module
https://avro.apache.org/docs/1.7.6/gettingstartedpython.html
"""

# futures
from __future__ import print_function, division

# system modules
import os
import json
import gzip
import itertools
from types import GeneratorType

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Utils import tstamp, wmaHash

class AvroStorage(Storage):
    "Storage based on Avro file based back-end"
    def __init__(self, uri):
        # avro uri: avro:/path/schema.avsc
        schema = uri.replace('avroio:', '')
        uripath, _ = schema.rsplit('/', 1)
        if  not os.path.exists(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        Storage.__init__(self, uripath)
        print(tstamp('WMA AvroIO storage'), uri)
        if  not os.path.exists(self.uri):
            os.makedirs(self.uri)
        self.schema = avro.schema.parse(open(schema).read())

    def write(self, data):
        "Write API"
        fname = '%s/%s.avro.gz' % (self.uri, wmaHash(data))
        print(tstamp('WMA AvroIO::write'), fname, data)
        with gzip.open(fname, 'w') as ostream:
            with DataFileWriter(ostream, DatumWriter(), self.schema) as writer:
                if  isinstance(data, list) or isinstance(data, GeneratorType):
                    for rec in data:
                        writer.append(rec)
                elif isinstance(data, dict):
                    writer.append(data)

    def read(self, query=None):
        "Read API"
        pass
