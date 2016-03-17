#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : SparkIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Spark storage client. This module is responsible
for providing read api to HDFS via Spark interface.
"""

# futures
from __future__ import print_function, division

# avro modules
import avro.schema
import avro.io

# hdfs pydoop modules
import pydoop.hdfs as hdfs

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError

class SparkStorage(Storage):
    "Storage based on HDFS/Spark back-end"
    def __init__(self, uri):
        "ctor with spark uri: sparkio://hdfspath/schema.avsc"
        Storage.__init__(self, uri)
        schema = self.uri
        if  not hdfs.ls(schema):
            raise Exception("No avro schema file found in provided uri: %s" % uri)
        self.hdir = self.uri.rsplit('/', 1)[0]
        if  not hdfs.path.isdir(self.hdir):
            raise Exception('HDFS path %s does not exists' % self.hdir)
        schema_doc = hdfs.load(schema)
        self.schema = avro.schema.parse(schema_doc)

    def write(self, data, safe=None):
        "Write API, return ids of stored documents"
        raise NotImplementedError

    def read(self, spec, fields=None):
        "Read API, it reads data from HDFS/Spark storage for provided spec."
        try:
            gen = self.find(spec, fields)
            docs = [r for r in gen]
            return docs
        except Exception as exp:
            raise ReadError(str(exp))

    def find(self, spec, fields):
        """
        Find records in HDFS/Spark storage for provided spec, returns generator
        """
        if  not spec:
            spec = {}
        if  isinstance(spec, list):
            spec = {'wmaid': {'$in': spec}}
        elif  PAT_UID.match(str(spec)):
            spec = {'wmaid': spec}
        if  fields:
            return self.coll.find(spec, fields)
        return self.coll.find(spec)
