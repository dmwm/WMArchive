#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : STSManager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Short-Term Storage manager. This module is responsible
for providing read/write APIs to Short-Term Storage.
"""

# futures
from __future__ import print_function, division

# WMArchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Storage.FileIO import FileStorage
from WMArchive.Storage.AvroIO import AvroStorage

class STSManager(object):
    "Short-Term Storage manager based on Mongo/File/Avro storage back-end"
    def __init__(self, uri):
        "ctor with STS uri"
        if  uri.startswith('mongo'):
            self.mgr = MongoStorage(uri)
        elif uri.startswith('file'):
            self.mgr = FileStorage(uri)
        elif uri.startswith('avro'):
            self.mgr = AvroStorage(uri)
        else:
            self.mgr = FileStorage(os.getenv('WMA_STORAGE_ROOT', '/tmp/wma_storage'))
        self.stype = self.mgr.stype # determine storage type

    def qmap(self, spec, fields):
        "map input spec/fields into ones suitable for STS QL"
        return spec, fields

    def write(self, data, safe=None):
        "Write API for STS"
        return self.mgr.write(data, safe)

    def read(self, spec, fields=None):
        "Read API for STS"
        return self.mgr.read(spec, fields)

    def jobs(self):
        "Fetch jobs ID from underlying storage"
        return self.mgr.jobsids()

    def stats(self):
        "Return statistics about underlying storage"
        return self.mgr.stats()

    def status(self):
        "Return status api"
        return {'sts':{'mgr':str(self.mgr), 'stype':self.stype, 'stats':self.stats()}}
