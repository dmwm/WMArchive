#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : HdfsIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive HDFS storage module based on pydoop python module

pydoop HDFS docs:
http://crs4.github.io/pydoop/api_docs/hdfs_api.html#hdfs-api
http://crs4.github.io/pydoop/tutorial/hdfs_api.html#hdfs-api-tutorial
http://stackoverflow.com/questions/23614588/encode-an-object-with-avro-to-a-byte-array-in-python

python gzip: https://docs.python.org/2/library/gzip.html
python io: https://docs.python.org/2/library/io.html

Usage of GzipFile with file-like object, e.g. io.BytesIO
http://stackoverflow.com/questions/4204604/how-can-i-create-a-gzipfile-instance-from-the-file-like-object-that-urllib-url
"""

# futures
from __future__ import print_function, division

# system modules
import io
import gzip

# avro modules
import avro.schema
import avro.io

# hdfs pydoop modules
try:
    import pydoop.hdfs as hdfs
except: # wrap for sphinx
    pass

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Utils import today, file_name
from WMArchive.Utils.Regexp import PAT_UID

class HdfsStorage(Storage):
    "Storage based on Hdfs back-end"
    def __init__(self, uri, compress=True):
        "ctor with hdfs uri: hdfsio:/path/schema.avsc"
        Storage.__init__(self, uri)
        schema = self.uri
        if  not hdfs.ls(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not hdfs.path.isdir(self.hdir):
            raise Exception('HDFS path %s does not exists' % self.hdir)
        schema_doc = hdfs.load(schema)
        self.schema = avro.schema.parse(schema_doc)
        self.compress = compress

    def dump(self, data, fname):
        "Dump given data directly to HDFS"
        hdfs.dump(data, fname)

    def _write(self, data):
        "Internal Write API"
        schema = self.schema
        wmaid = self.wmaid(data)
        year, month, _ = today()
        hdir = '%s/%s/%s' % (self.hdir, year, month)
        if  not hdfs.path.isdir(hdir):
            hdfs.mkdir(hdir)
        fname = file_name(hdir, wmaid, self.compress)

        # create Avro writer and binary encoder
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()

        if  self.compress:
            # use gzip'ed writer with BytesIO file object
            gzip_writer = gzip.GzipFile(fileobj=bytes_writer, mode='wb')
            encoder = avro.io.BinaryEncoder(gzip_writer)
        else:
            # plain binary reader
            encoder = avro.io.BinaryEncoder(bytes_writer)

        # write records from given data stream to binary writer
        writer.write(data, encoder)

        # close gzip stream if necessary
        if  self.compress:
            gzip_writer.flush()
            gzip_writer.close()

        # store raw data to hadoop via HDFS
        hdfs.dump(bytes_writer.getvalue(), fname)

        # close bytes stream
        bytes_writer.close()

    def _read(self, spec, fields=None):
        "Internal read API"
        if  PAT_UID.match(str(spec)): # requested to read concrete file
            out = []
            year, month, _ = today()
            hdir = '%s/%s/%s' % (self.hdir, year, month)
            fname = file_name(hdir, spec, self.compress)
            data = hdfs.load(fname)
            bytes_reader = io.BytesIO(data)

            if  self.compress:
                # use gzip'ed reader and pass to it BytesIO as file object
                gzip_reader = gzip.GzipFile(fileobj=bytes_reader)
                decoder = avro.io.BinaryDecoder(gzip_reader)
            else:
                # use non-compressed reader
                decoder = avro.io.BinaryDecoder(bytes_reader)

            reader = avro.io.DatumReader(self.schema)
            while True:
                try:
                    rec = reader.read(decoder)
                    out.append(rec)
                except:
                    break
            # close gzip stream if necessary
            if  self.compress:
                gzip_reader.close()
            # close bytes stream
            bytes_reader.close()
            return out
        return self.empty_data
