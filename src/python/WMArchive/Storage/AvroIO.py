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
import io
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
from WMArchive.Utils.Regexp import PAT_UID

def fileName(uri, wmaid):
    "Construct common file name"
    return '%s/%s.avro' % (uri, wmaid)

class AvroStorage(Storage):
    "Storage based on Avro file based back-end"
    def __init__(self, uri):
        "ctor with avro uri: avroio:/path/schema.avsc"
        Storage.__init__(self, uri)
        schema = self.uri
        if  not os.path.exists(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        if  not os.path.exists(self.uri):
            os.makedirs(self.uri)
        self.schema = avro.schema.parse(open(schema).read())

    def _write(self, data):
        "Internal write API"
        wmaid = data['wmaid']
        fname = fileName(self.uri, wmaid)
        with open(fname, 'w') as ostream:
            with DataFileWriter(ostream, DatumWriter(), self.schema) as writer:
                writer.append(data)

    def _read(self, query=None):
        "Internal read API"
        if  PAT_UID.match(str(query)): # requested to read concrete file
            out = []
            fname = fileName(self.uri, query)
            with open(fname) as istream:
                reader = DataFileReader(istream, DatumReader())
                for rec in reader:
                    print("rec", rec)
                    self.check(rec)
                    out.append(rec)
            return out
        return self.empty_data
