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
from WMArchive.Utils.Utils import wmaHash, tstamp
from WMArchive.Utils.Exceptions import WriteError, ReadError

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
        self._version = "1.0.0"

    def info(self):
        "Return info about WMArchive"
        return {'WMArchive' : {'version': self._version}}

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

    def read(self, query):
        """
        Send request to proxy server to read data for given query.
        Yield list of found documents or None.
        """
        status = 'ok'
        try:
            # request data from back-end
            data = self.mgr.read(query)
        except ReadError as exp:
            print(exp)
            data = []
            status = 'read error'
        except Exception as exp:
            data = []
            print(tstamp("WMArchiveManager::write"), "fail with %s" % str(exp))
            status = 'fail'
        result = {'query': query, 'data': data, 'stype': self.mgr.stype, 'status': status}
        return result
