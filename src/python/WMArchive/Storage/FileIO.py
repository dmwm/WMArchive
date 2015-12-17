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
from WMArchive.Utils.Regexp import PAT_UID

class FileStorage(Storage):
    "Storage based on FileDB back-end"
    def __init__(self, uri):
        "ctor with fileio uri: fileio:/path"
        self.log(uri)
        uri = uri.replace('fileio:', '')
        Storage.__init__(self, uri)
        if  not os.path.exists(uri):
            os.makedirs(self.uri)

    def _write(self, data):
        "Internal write API"
        wmaid = data['wmaid']
        fname = '%s/%s.gz' % (self.uri, wmaid)
        with gzip.open(fname, 'w') as ostream:
            ostream.write(json.dumps(data))

    def _read(self, query=None):
        "Internal read API"
        if  PAT_UID.match(query): # requested to read concrete file
            fname = '%s/%s.gz' % (self.uri, query)
            data = json.load(gzip.open(fname))
            self.check(data)
            return [data]
        return self.empty_data
