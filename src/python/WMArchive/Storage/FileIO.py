#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : FileIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive File storage module
"""

# futures
from __future__ import print_function, division

# system modules
import os
import json
import itertools

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Utils import tstamp, wmaHash

class FileStorage(Storage):
    "Storage based on FileDB back-end"
    def __init__(self, uri=None):
        if  not uri:
            uri = os.getenv('WMA_STORAGE_ROOT', '/tmp/wma_storage')
        uri = uri.replace('fileio:', '')
        Storage.__init__(self, uri)
        print(tstamp('WMA FileIO storage'), self.uri)
        if  not os.path.exists(uri):
            os.makedirs(self.uri)

    def write(self, data):
        "Write API"
        fname = '%s/%s' % (self.uri, wmaHash(data))
        print(tstamp('WMA FileIO::write' % self.uri), fname, data)
        with open(fname, 'w') as ostream:
            ostream.write(json.dumps(data))

    def read(self, query=None):
        "Read API"
        pass
