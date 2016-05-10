#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : BaseIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Base class to define storage APIs. It contains individual
_read/_write methods for single record or bulk read/write methods for list of
records.  The subclasses can either implement _read/_write or read/write
methods.
"""

# futures
from __future__ import print_function, division

# system modules
import traceback
from types import GeneratorType

# WMArchive modules
from WMArchive.Utils.Utils import wmaHash, tstamp
from WMArchive.Utils.Exceptions import ReadError, WriteError

class Storage(object):
    "Base class which defines storage APIs"
    def __init__(self, uri=None):
        if  not uri:
            raise Exception('Unable to init storage, invalid uri %s' % uri)
        self.log(uri)
        self.stype, self.uri = uri.split(':', 1)
        self.empty_data = [] # we will always return a list

    def log(self, msg):
        "Write given message to log stream"
        print(tstamp(self.__class__.__name__), msg)

    def wmaid(self, data):
        "Return wmaid for given data record or list of records"
        if  isinstance(data, list) or isinstance(data, GeneratorType):
            wmaids = self.getids(data)
            wmaid = wmaHash('_'.join(wmaids))
        else:
            wmaid = data['wmaid']
        return wmaid

    def _write(self, data):
        "Internal write API, should be implemented in subclasses"
        pass

    def write(self, data, safe=False):
        """
        Write API, return ids of stored documents. All documents are writen
        via self._write API one by one.
        """
        try:
            wmaids = self.getids(data)
            if  isinstance(data, list) or isinstance(data, GeneratorType):
                for rec in data:
                    self.log('write %s' % rec['wmaid'])
                    self._write(rec)
            elif isinstance(data, dict):
                self.log('write %s' % data['wmaid'])
                self._write(data)
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            msg = 'Failure in %s storage, error=%s' % (self.stype, err)
            raise WriteError(msg)

        # if safe argument is provided we'll read data again and check it
        if  safe:
            if  isinstance(data, dict):
                data = [data]
            docs = self.read(wmaids)
            for rec1, rec2 in zip(data, docs):
                for attr in ['wmaid', 'stype']:
                    if  attr in rec1:
                        del rec1[attr]
                    if  attr in rec2:
                        del rec2[attr]
                if rec1 != rec2:
                    raise Exception('Data mismatch: %s %s' % (rec1, rec2))
        return wmaids

    def _read(self, spec, fields=None):
        "Internal read API, should be implemented in subclasses"
        return self.empty_data

    def read(self, spec, fields=None):
        "Read data from local storage for given spec"
        try:
            if  isinstance(spec, list):
                out = []
                for item in spec:
                    res = self._read(item, fields)
                    if  res and len(res) == 1:
                        out.append(res[0])
                return out
            data = self._read(spec, fields)
            return data
        except Exception as exc:
            err = traceback.format_exc(limit=1).splitlines()[-1]
            msg = 'Failure in %s storage, error=%s' % (self.stype, err)
            raise ReadError(msg)

    def update(self, ids, spec):
        "Update documents with given set of document ids and update spec"
        pass

    def getids(self, data):
        "Return list of wmaids for given data"
        if  isinstance(data, list) or isinstance(data, GeneratorType):
            return [r['wmaid'] for r in data]
        return [data['wmaid']]

    def check(self, data):
        "Cross-check the data based on its wmaid"
        try:
            wmaid = data.pop('wmaid')
        except:
            wmaid = ''
        if  'stype' in data:
            del data['stype']
        hid = wmaHash(data)
        if  hid != wmaid:
            raise Exception("Invalid data hash, hid=%s, wmaid=%s, data=%s" \
                    % (hid, wmaid, data))
