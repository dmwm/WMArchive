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
import gzip
import itertools
from types import GeneratorType

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Utils import tstamp
from WMArchive.Utils.Regexp import PAT_UID

class FileStorage(Storage):
    "Storage based on FileDB back-end"
    def __init__(self, uri):
        uri = uri.replace('fileio:', '')
        Storage.__init__(self, uri)
        print(tstamp('WMA FileIO storage'), self.uri)
        if  not os.path.exists(uri):
            os.makedirs(self.uri)

    def write(self, data):
        "Write API, return ids of stored documents"
        wmaids = []
        if  isinstance(data, list) or isinstance(data, GeneratorType):
            for rec in data:
                wmaid = rec['wmaid']
                fname = '%s/%s.gz' % (self.uri, wmaid)
                with gzip.open(fname, 'w') as ostream:
                    ostream.write(json.dumps(rec))
                wmaids.append(wmaid)
        elif isinstance(data, dict):
            wmaid = data['wmaid']
            fname = '%s/%s.gz' % (self.uri, wmaid)
            with gzip.open(fname, 'w') as ostream:
                ostream.write(json.dumps(data))
            wmaids.append(wmaid)
        return wmaids

    def read(self, query=None):
        "Read API, return data"
        if  PAT_UID.match(query): # requested to read concrete file
            fname = '%s/%s.gz' % (self.uri, query)
            data = json.load(gzip.open(fname))
            self.check(data)
            return data
        return {}

    def update(self, ids, spec):
        "Update documents with given set of document ids and update spec"
        pass
