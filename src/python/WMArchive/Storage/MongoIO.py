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
import traceback

# Mongo modules
from pymongo import MongoClient
from pymongo.errors import InvalidDocument, InvalidOperation, DuplicateKeyError
from pymongo.son_manipulator import SONManipulator
from bson.son import SON

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Utils import tstamp

class WMASONManipulator(SONManipulator):
    """WMArchive MongoDB SON manipulator"""
    def __init__(self):
        SONManipulator.__init__(self)

    def transform_incoming(self, son, collection):
        "Manipulate an incoming SON object"
        if  self.will_copy():
            return SON(son)
        return son

    def transform_outgoing(self, son, collection):
        "Manipulate an outgoing SON object"
        if  self.will_copy():
            return SON(son)
        if  isinstance(son, dict) and '_id' in son:
            del son['_id']
        return son

class MongoStorage(Storage):
    "Storage based on MongoDB back-end"
    def __init__(self, uri, dbname='fwjr', collname='db', chunk_size=1000):
        "ctor with mongo uri: mongodb://host:port"
        Storage.__init__(self, uri)
        self.client = MongoClient(uri, w=1)
        self.mdb = self.client[dbname]
        self.mdb.add_son_manipulator(WMASONManipulator())
        self.coll = self.mdb[collname]
        print(tstamp('WMA INFO'), self.coll)
        self.chunk_size = chunk_size

    def write_bulk(self, data):
        "Bulk write API, return ids of stored documents"
        return self.write(data)

    def write(self, data):
        "Write API, return ids of stored documents"
        if  not isinstance(data, list):
            data = [data]
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
            print(traceback.print_exc())
            print(tstamp('WMA WARNING'), 'Uncaught exception', str(exp))
        return wmaids

    def read(self, query=None):
        "Read API"
        if  not query:
            query = {}
        if  isinstance(query, list):
            query = {'wmaid': {'$in': query}}
        elif  PAT_UID.match(str(query)):
            query = {'wmaid': query}
        docs = [r for r in self.coll.find(query)]
        return docs

    def update(self, ids, spec):
        "Update documents with given set of document ids and update spec"
        doc_query = {'wmaid' : {'$in': ids}}
        self.coll.update(doc_query, spec, multi=True)

    def remove(self, spec=None):
        "Remove documents from MongoDB for given spec"
        if  not spec:
            spec = {}
        self.coll.remove(spec)

    def dropdb(self, dbname):
        "Remove given database from MongoDB"
        self.client.drop_database(dbname)
