#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : MongoIO.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: WMArchive Mongo storage client
"""

# futures
from __future__ import print_function, division

# system modules
import itertools

# Mongo modules
from pymongo import MongoClient
from pymongo.errors import InvalidDocument, InvalidOperation, DuplicateKeyError

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID

class MongoStorage(Storage):
    "Storage based on MongoDB back-end"
    def __init__(self, uri):
        "ctor with mongo uri: mongodb://host:port"
        Storage.__init__(self, uri)
        self.client = MongoClient(uri)
        self.coll = self.client['fwjr']['db']
        self.chunk_size = 100

    def write_in_bluk(self, data):
        "Bulk write API, return ids of stored documents"
        return self.write(data)

    def write(self, data):
        "Write API, return ids of stored documents"
        wmaids = self.getids(data)
        try:
            while True:
                nres = self.coll.insert(itertools.islice(data, self.chunk_size))
                if  not nres:
                    break
        except InvalidDocument as exp:
            self.log('WARNING InvalidDocument: %s' % str(exp))
        except InvalidOperation as exp:
            self.log('WARNING InvalidOperation: %s' % str(exp))
        except DuplicateKeyError as exp:
            pass
        except Exception as exp:
            print(tstamp('WMA WARNING'), 'Uncaught exception', str(exp))
        return wmaids

    def read(self, query=None):
        "Read API"
        if  not query:
            query = {}
        docs = []
        if  isinstance(query, list):
            query = {'wmaid': {'$in': query}}
        elif  PAT_UID.match(str(query)):
            query = {'wmaid': query}
        for rec in self.coll.find(query):
            del rec['_id'] # internal MongoDB id
            docs.append(rec)
        return docs

    def update(self, ids, spec):
        "Update documents with given set of document ids and update spec"
        doc_query = {'wmaid' : {'$in': ids}}
        self.coll.update(doc_query, spec, multi=True)

    def remove(self, spec):
        "Remove documents from MongoDB for given spec"
        self.coll.remove(spec)
