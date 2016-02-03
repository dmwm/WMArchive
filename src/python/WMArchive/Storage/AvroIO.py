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
import traceback

# avro modules
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Utils import wmaHash, open_file, file_name
from WMArchive.Utils.Exceptions import WriteError, ReadError

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

    def file_write(self, fname, data):
        "Write documents in append mode to given file name"
        try:
            schema = self.schema
            wmaids = []
            if  not hasattr(data, '__iter__') or isinstance(data, dict):
                data = [data]

	    with DataFileWriter(open_file(fname, 'a'), DatumWriter(), schema) as writer:
		for rec in data:
		    writer.append(rec)
                    wmaid = rec.get('wmaid', wmaHash(rec))
                    wmaids.append(wmaid)
            return wmaids
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            msg = 'Failure in %s storage, error=%s' % (self.stype, err)
            raise WriteError(msg)

    def file_read(self, fname):
        "Read documents from given file name"
        try:
            schema = self.schema
            out = []
            with DataFileReader(open_file(fname), DatumReader()) as reader:
                for rec in reader:
                    out.append(rec)
            return out
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            msg = 'Failure in %s storage, error=%s' % (self.stype, err)
            raise ReadError(msg)

    def _write(self, data):
        "Internal write API"
        wmaid = self.wmaid(data)
        schema = self.schema
        fname = file_name(self.hdir, wmaid)
        with open_file(fname, 'w') as ostream:
            with DataFileWriter(ostream, DatumWriter(), schema) as writer:
                writer.append(data)

    def _read(self, query=None):
        "Internal read API"
        if  PAT_UID.match(str(query)): # requested to read concrete file
            out = []
            fname = file_name(self.hdir, query)
            with open_file(fname) as istream:
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
