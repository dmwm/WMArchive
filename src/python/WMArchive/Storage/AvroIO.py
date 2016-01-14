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
from WMArchive.Utils.Utils import bulk_avsc, bulk_data

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
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not os.path.exists(self.hdir):
            os.makedirs(self.hdir)
        schema_doc = open(schema).read()
        self.schema = avro.schema.parse(schema_doc)
        self.schema_bulk = avro.schema.parse(json.dumps(bulk_avsc(schema_doc)))

    def file_write(self, fname, data):
        "Write documents in append mode to given file name"
        schema = self.schema
        if  not hasattr(data, '__iter__'):
            data = [data]
        with open(fname, 'a') as ostream:
            with DataFileWriter(ostream, DatumWriter(), schema) as writer:
                for rec in data:
                    wmaid = rec['wmaid']
                    writer.append(rec)
                    writer.flush()
                    yield wmaid

    def file_read(self, fname):
        "Read documents from given file name"
        schema = self.schema
        if  fname.endswith('.gz'):
            istream = gzip.open(fname)
        else:
            istream = open(fname)
        out = []
        with istream:
            reader = DataFileReader(istream, DatumReader())
            try:
                for data in reader:
                    out.append(data)
            except UnicodeDecodeError:
                pass
        return out


    def _write(self, data, bulk=False):
        "Internal write API"
        wmaid = self.wmaid(data)
        schema = self.schema
        if  bulk:
            schema = self.schema_bulk
            data = bulk_data(data)
        fname = fileName(self.hdir, wmaid)
        with open(fname, 'w') as ostream:
            with DataFileWriter(ostream, DatumWriter(), schema) as writer:
                writer.append(data)

    def _read(self, query=None):
        "Internal read API"
        if  PAT_UID.match(str(query)): # requested to read concrete file
            out = []
            fname = fileName(self.hdir, query)
            with open(fname) as istream:
                reader = DataFileReader(istream, DatumReader())
                for data in reader:
                    if  isinstance(data, list):
                        for rec in data:
                            self.check(rec)
                        return data
                    elif isinstance(data, dict) and 'bulk' in data:
                        for rec in data['bulk']:
                            self.check(rec)
                        return data
                    self.check(data)
                    out.append(data)
            return out
        return self.empty_data
