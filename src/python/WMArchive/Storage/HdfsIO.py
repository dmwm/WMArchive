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
import os
import io
import gzip
import itertools
from types import GeneratorType

# avro modules
import avro.schema
import avro.io

# hdfs pydoop modules
import pydoop.hdfs as hdfs

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Utils import tstamp, wmaHash
from WMArchive.Utils.Regexp import PAT_UID

def fileName(uri, uid):
    "Construct common file name"
    return '%s/%s.avro.gz' % (uri, uid)

class HdfsStorage(Storage):
    "Storage based on Hdfs back-end"
    def __init__(self, uri):
        # hdfs uri: hdfsio:/path/schema.avsc
        schema = uri.replace('hdfsio:', '')
        uripath, _ = schema.rsplit('/', 1)
        if  not os.path.exists(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        Storage.__init__(self, uripath)
        print(tstamp('WMA HdfsIO storage'), uri)
        if  not hdfs.path.isdir(self.uri):
            hdfs.mkdir(self.uri)
        self.schema = avro.schema.parse(open(schema).read())

    def write(self, data):
        "Write API"
        fname = fileName(self.uri, wmaHash(data))
        print(tstamp('WMA HdfsIO::write'), fname, data)

        # create Avro writer and binary encoder
	writer = avro.io.DatumWriter(self.schema)
	bytes_writer = io.BytesIO()

        # example of plain binary reader
        # encoder = avro.io.BinaryEncoder(bytes_writer)

        # use gzip'ed writer with BytesIO file object
        gzip_writer = gzip.GzipFile(fileobj=bytes_writer, mode='wb')
	encoder = avro.io.BinaryEncoder(gzip_writer)

        # write records from given data stream to binary writer
	if  isinstance(data, list) or isinstance(data, GeneratorType):
	    for rec in data:
		writer.write(rec, encoder)
	else:
	    writer.write(data, encoder)

	# store raw data to hadoop via HDFS
	hdfs.dump(bytes_writer.getvalue(), fname)

    def read(self, query=None):
        "Read API"
        out = []
        if  PAT_UID.match(query): # requested to read concrete file
            fname = fileName(self.uri, query)
            data = hdfs.load(fname)

            # example of non-zipped reader
            # bytes_reader = io.BytesIO(data)
            # decoder = avro.io.BinaryDecoder(bytes_reader)

            # use gzip'ed reader and pass to it BytesIO as file object
            gzip_reader = gzip.GzipFile(fileobj=io.BytesIO(data))
            decoder = avro.io.BinaryDecoder(gzip_reader)
            reader = avro.io.DatumReader(self.schema)
            while True:
                rec = reader.read(decoder)
                out.append(rec)
            except:
                break
        return out
