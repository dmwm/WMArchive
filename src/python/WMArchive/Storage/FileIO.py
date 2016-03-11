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

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Utils import open_file

class FileStorage(Storage):
    "Storage based on FileDB back-end"
    def __init__(self, uri):
        "ctor with fileio uri: fileio:/path"
        Storage.__init__(self, uri)
        if  not os.path.exists(self.uri):
            os.makedirs(self.uri)

    def _write(self, data):
        "Internal write API"
        wmaid = self.wmaid(data)
        fname = '%s/%s.gz' % (self.uri, wmaid)
        with open_file(fname, 'w') as ostream:
            ostream.write(json.dumps(data))

    def _read(self, spec, fields=None):
        "Internal read API"
        if  PAT_UID.match(str(spec)): # requested to read concrete file
            fname = '%s/%s.gz' % (self.uri, spec)
            data = json.load(open_file(fname))
            if  isinstance(data, list):
                for rec in data:
                    self.check(rec)
                return data
            self.check(data)
            return [data]
        return self.empty_data
