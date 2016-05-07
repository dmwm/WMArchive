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
from pymongo import MongoClient, DESCENDING
from pymongo.errors import InvalidDocument, InvalidOperation, DuplicateKeyError
from pymongo.son_manipulator import SONManipulator
from bson.son import SON

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError

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
        self.log(self.coll)
        self.chunk_size = chunk_size
        try:
            self.coll.ensure_index([('wmaid', DESCENDING)], unique=True)
            self.coll.ensure_index([('wmats', DESCENDING), ('stype', DESCENDING)])
        except:
            pass

    def write(self, data, safe=None):
        "Write API, return ids of stored documents"
        if  not isinstance(data, list):
            data = [data] # ensure that we got list of data
        wmaids = self.getids(data)
        total = nres = 0
        for idx in range(0, len(data), self.chunk_size):
            docs = data[idx:idx+self.chunk_size]
            try:
                nres = self.coll.insert(docs, continue_on_error=True)
            except InvalidDocument as exp:
                self.log('WARNING InvalidDocument: %s' % str(exp))
            except InvalidOperation as exp:
                self.log('WARNING InvalidOperation: %s' % str(exp))
            except DuplicateKeyError as exp:
                pass
            except Exception as exp:
                raise WriteError(str(exp))
            total += len(nres)
        if  total != len(wmaids):
            msg = 'Unable to insert all records, given (%s) != inserted (%s)' \
                    % (len(wmaids), total)
            self.log('WARNING %s' % msg)
        return wmaids

    def read(self, query=None):
        "Read API, it reads data from MongoDB storage for provided query."
        try:
            gen = self.find(query)
            docs = [r for r in gen]
            return docs
        except Exception as exp:
            raise ReadError(str(exp))

    def find(self, query=None):
        """
        Find records in MongoDB storage for provided query, returns generator
        over MongoDB collection
        """
        if  not query:
            query = {}
        if  isinstance(query, list):
            query = {'wmaid': {'$in': query}}
        elif  PAT_UID.match(str(query)):
            query = {'wmaid': query}
        return self.coll.find(query)

    def update(self, ids, spec):
        "Update documents with given set of document ids and update spec"
        if  len(ids) > self.chunk_size:
            for idx in range(0, len(ids), self.chunk_size):
                sub_ids = ids[idx:idx+self.chunk_size]
                doc_query = {'wmaid' : {'$in': sub_ids}}
                self.coll.update(doc_query, spec, multi=True)
        else:
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
