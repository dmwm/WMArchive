#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : Manager.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchiveManager class provides full functionality of the WMArchive service.

TODO: This class will be expanded/modified according to requirements
of WMArchive project, see
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMArchive
"""

# futures
from __future__ import print_function, division

# system modules
import os
import time

# WMArchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Storage.FileIO import FileStorage
from WMArchive.Storage.AvroIO import AvroStorage
try:
    from WMArchive.Storage.SparkIO import SparkStorage
    LTS = True
except:
    LTS = False
from WMArchive.Utils.Utils import wmaHash, tstamp
from WMArchive.Utils.Exceptions import WriteError, ReadError

def use_lts(trange):
    """
    Helper function to determine based on given time range either
    to use Short Term Storage or Long Term Storage
    """
    # we need to implement the logic, trange is tuple of two values
    return False

class WMArchiveManager(object):
    """
    Initialize WMArchive proxy server configuration. The given configuration
    file will provide details of proxy server, agent information, etc.
    """
    def __init__(self, config=None):
        self.config = config
        if  config.short_storage_uri.startswith('mongo'):
            self.mgr = MongoStorage(config.short_storage_uri)
        elif config.short_storage_uri.startswith('file'):
            self.mgr = FileStorage(config.short_storage_uri)
        elif config.short_storage_uri.startswith('avro'):
            self.mgr = AvroStorage(config.short_storage_uri)
        else:
            self.mgr = FileStorage(os.getenv('WMA_STORAGE_ROOT', '/tmp/wma_storage'))
        # Short-Term Storage
        self.sts = self.mgr
        # Long-Term Storage
        if  LTS: # we'll use this module if it's loaded
            self.lts = SparkStorage(config.long_storage_uri)
        else: # fallback
            self.lts = self.mgr
        self.specmap = {}
        with open(config.specmap, 'r') as istream:
            cdict = {}
            for line in istream.readlines():
                pair = line.replace('\n', '').split(',')
                self.specmap[pair[0]] = pair[1] # lfn:LFNArray
        print("WMArchive::Manager specmap", self.specmap)

    def sconvert(self, mgr, spec, fields):
        "Convert user based spec into WMArhchive storage one"
        newspec = {}
        newfields = []
        for key, val in spec.items():
            newspec[self.specmap.get(key, key)] = val
        for field in fields:
            newfields.append(self.specmap.get(fields, field))
        if  hasattr(mgr, 'sconvert'):
            return mgr.sconvert(newspec, fields)
        return newspec, newfields

    def encode(self, docs):
        """
        Encode given set of documents into appropriate format for long term storage.
        This method will consume documents in DMWM JSON format.
        Yield encoded documents to the client.
        """
        for doc in docs:
            doc['wmaid'] = wmaHash(doc)
            doc['wmats'] = time.time()
            doc['stype'] = self.mgr.stype
            yield doc

    def decode(self, docs):
        """
        Decode given set of documents into DMWM JSON format.
        Yield decoded documents to the client.
        """
        for doc in docs:
            yield doc

    def write(self, data):
        """
        Write given data chunk (list of WM documents) into proxy server.
        Return true or false of write operation.
        """
        status = 'ok'
        ids = []
        try:
            if  isinstance(data, dict):
                data = [data]
            if  not isinstance(data, list):
                raise Exception("WMArchiveManager::write, Invalid data format: %s" % type(data))
            docs = [r for r in self.encode(data)]
            ids = self.mgr.write(docs)
            if  not ids and len(data): # somehow we got empty list for given data
                status = 'unknown'
        except WriteError as exp:
            print(exp)
            data = []
            status = 'write error'
        except Exception as exp:
            print(tstamp("WMArchiveManager::write"), "fail with %s" % str(exp))
            status = 'fail'
            ids = []
        result = {'stype': self.mgr.stype, 'ids': ids, 'status': status}
        return result

    def read(self, spec, fields):
        """
        Send request to proxy server to read data for given query.
        Yield list of found documents or None.
        """
        # convert given spec into query suitable for sts/lts
        try:
            trange = spec.pop('timerange')
        except KeyError:
            data = []
            print(tstamp("WMArchiveManager::read"), "timerange is not provided")
            status = 'fail'
            reason = 'No timerange is provided, please adjust your query spec'
            result = {'input': {'spec': spec, 'fields': fields},
                      'results': data, 'storage': self.mgr.stype,
                      'status': status, 'reason': reason}
            return result

        # based on given time range define which manager
        # we'll use for data look-up
        mgr = self.sts
        if  use_lts(trange):
            mgr = self.lts

        # convert spec into WMArchive one
        spec, fields = self.sconvert(mgr, spec, fields)
        status = 'ok'
        reason = None
        try:
            # request data from back-end
            data = mgr.read(spec, fields)
        except ReadError as exp:
            print(exp)
            data = []
            status = 'read error'
        except Exception as exp:
            data = []
            print(tstamp("WMArchiveManager::read"), "fail with %s" % str(exp))
            reason = str(exp)
            status = 'fail'
        result = {'input': {'spec': spec, 'fields': fields},
                  'results': data, 'storage': self.mgr.stype, 'status': status}
        if  reason:
            result['reason'] = reason
        return result
