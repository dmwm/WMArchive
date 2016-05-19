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
import json
import traceback

# Mongo modules
from pymongo import MongoClient
from pymongo.errors import InvalidDocument, InvalidOperation, DuplicateKeyError
from pymongo.son_manipulator import SONManipulator
from bson.son import SON

# WMArchive modules
from WMArchive.Storage.BaseIO import Storage
from WMArchive.Utils.Regexp import PAT_UID
from WMArchive.Utils.Exceptions import WriteError, ReadError

def set_duplicates(docs):
    "Return duplicates FWJR ids within given set of docs"
    fwjrids = []
    for rec in docs:
        if  isinstance(rec, dict):
            fid = rec.get('meta_data', {}).get('fwjr_id', -1)
        else:
            fid = -1
        fwjrids.append(fid)
    dups = set([x for x in fwjrids if fwjrids.count(x) > 1])
    return list(dups)

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
        self.collname = collname
        self.coll = self.mdb[collname]
        self.jobs = self.mdb['jobs'] # separate collection for job results
        self.acol = self.mdb['acol'] # separate collection for aggregated results
        self.log(self.coll)
        self.chunk_size = chunk_size

    def sconvert(self, spec, fields):
        "convert input spec/fields into ones suitable for MognoDB QL"
        return spec, fields

    def find_duplicates(self, wmaids):
        "Find duplicate FWJR ids from collection of docs"
        spec = {'wmaid':{'$in':wmaids}}
        fields = ['meta_data']
        fwjr_ids = []
        for doc in self.coll.find(spec, fields):
            fwjr_ids.append(doc['meta_data'])
        return fwjr_ids

    def write(self, data, safe=None):
        "Write API, return ids of stored documents"
        if  not isinstance(data, list):
            data = [data] # ensure that we got list of data
        coll = self.coll
        if  isinstance(data[0], dict) and data[0].get('dtype', None) == 'job':
            coll = self.jobs
        wmaids = self.getids(data)
        uniqids = set(wmaids)
        sts_dup = self.find_duplicates(wmaids)
        if  len(wmaids) != len(uniqids):
            set_dup = set_duplicates(data)
            self.log("WARNING, found %s duplicates in given docs, FWJR ids %s, given %s, unique %s" \
                    % ( len(wmaids)-len(uniqids), json.dumps(set_dup), len(wmaids), len(set(wmaids)) ) )
        if  sts_dup:
            self.log("WARNING, found %s duplicates in STS:" % len(sts_dup))
            for rec in sts_dup:
                self.log('WARNING, duplicate record %s' % json.dumps(rec))
        for idx in range(0, len(data), self.chunk_size):
            docs = data[idx:idx+self.chunk_size]
            try:
                self.coll.insert(docs, continue_on_error=True)
            except InvalidDocument as exp:
                self.log('WARNING InvalidDocument: %s' % str(exp))
            except InvalidOperation as exp:
                self.log('WARNING InvalidOperation: %s' % str(exp))
            except DuplicateKeyError as exp:
                pass
            except Exception as exp:
                raise WriteError(str(exp))
        return wmaids

    def read(self, spec, fields=None):
        "Read API, it reads data from MongoDB storage for provided spec."
        try:
            gen = self.find(spec, fields)
            docs = [r for r in gen]
            return docs
        except Exception as exp:
            raise ReadError(str(exp))

    def find(self, spec, fields):
        """
        Find records in MongoDB storage for provided spec, returns generator
        over MongoDB collection
        """
        if  not spec:
            spec = {}
        if  isinstance(spec, list):
            spec = {'wmaid': {'$in': spec}}
            return self.jobs.find(spec)
        elif  PAT_UID.match(str(spec)):
            spec = {'wmaid': spec}
            return self.jobs.find(spec)
        if  fields:
            return self.coll.find(spec, fields)
        return self.coll.find(spec)

    def ndocs(self, spec):
        "Return number of documents for given spec"
        return self.coll.find(spec).count()

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
        return self.coll.remove(spec)

    def dropdb(self, dbname):
        "Remove given database from MongoDB"
        return self.client.drop_database(dbname)

    def stats(self):
        "Return statistics about MongoDB"
        return self.mdb.command("collstats", self.collname)

    def jobsids(self):
        "Return jobs ids"
        out = []
        for row in self.jobs.find():
            if  'wmaid' in row:
                out.append({'wmaid':row['wmaid']})
        return out

#     def adocs(self):
#         "Return aggregated statistics documents"
#         return [d for d in self.acol.find()]

# Below is an example of how we can extract aggregated docs, convert them
# to Charts.js data format and return the data. This data can be used
# by WMArchive web interface, see ajaxRequestAdocsExample() Javascript
# function which feeds a data to a bar plot on web interface, see stats.tmpl
    def adocs(self):
        "Return aggregated documents in chart.js representation"
        docs = {}
        for row in self.acol.find():
            docs.update(row)
        out = {"labels":docs.keys(), "datasets":[]}
        values = []
        for key in out['labels']:
            site_stats = docs[key]
            values.append(site_stats['storage']['writeTotalMB'])
        out['datasets'].append(dict(data=values))
        return out
