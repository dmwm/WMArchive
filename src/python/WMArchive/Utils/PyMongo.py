#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : wma_pymongo.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: helper module to deal with pymongo options
"""

# futures
from __future__ import print_function, division

# system modules
import time
import traceback

# Mongo modules
import pymongo
from pymongo import MongoClient
from pymongo.errors import AutoReconnect, ConnectionFailure
from pymongo import version as pymongo_version

# WMArchive modules
from WMArchive.Utils.Utils import Singleton

# pymongo driver changed usage of its options here we declare them
# based on its version
PYMVER = pymongo_version.split('.')[0]
if  PYMVER == '2':
    PYMONGO_OPTS = {'exhaust': True}
    PYMONGO_NOEXHAUST = {'exhaust': False}
    class MongoOpts(object):
        """Class which holds MongoClient options"""
        def __init__(self, **kwds):
            self.write = kwds.get('w', 1)
            self.psize = kwds.get('psize', 300)
        def opts(self):
            "Return MongoClient options"
            return dict(w=self.write, max_pool_size=self.psize)
elif  PYMVER == '3':
    from pymongo.cursor import CursorType
    PYMONGO_OPTS = {'cursor_type': CursorType.EXHAUST}
    PYMONGO_NOEXHAUST = {'cursor_type': CursorType.NON_TAILABLE}
    class MongoOpts(object):
        """Class which holds MongoClient options"""
        def __init__(self, **kwds):
            self.write = kwds.get('w', 1)
            self.psize = kwds.get('psize', 300)
        def opts(self):
            "Return MongoClient options"
            return dict(w=self.write, maxPoolSize=self.psize)
else:
    raise Exception('Unsupported pymongo version, %s' % pymongo_version)

class MongoConnection(object):
    "MongoConnection class provide access to MongoClient"
    __metaclass__ = Singleton # in python3 use MongoConnection(object, metaclass=Singleton)
    def __init__(self, uri, **args):
        self.mongo_client = MongoClient(uri, **args)
    def client(self):
        "Return mongo client"
        return self.mongo_client

class DBConnection(object):
#    __metaclass__ = Singleton # in python3 use DBConnection(object, metaclass=Singleton)
    """
    DB Connection class which hanldes MongoDB connections. Input parameters:

        - lifetime, controls connection lifetime
        - retry, controls number of retries to acquire MongoDB connection
    """
    def __init__(self, pool_size=300, lifetime=3600, retry=5):
        # just for the sake of information
        self.instance = "Instance at %d" % self.__hash__()
        self.conndict = {}
        self.timedict = {}
        self.thr = lifetime
        self.retry = retry
        self.psize = pool_size
        self.mongo_opts = MongoOpts(w=1, psize=self.psize).opts()
        print("### pymongo version %s" % pymongo.version)

    def genkey(self, uri):
        "Generate unique key"
        if  isinstance(uri, basestring):
            key = uri
        elif isinstance(uri, list) and len(uri) == 1:
            key = uri[0]
        else:
            key = str(uri)
        return key

    def connection(self, uri):
        """Return MongoDB connection"""
        key = self.genkey(uri)
        # check cache first
        try: # this block may fail in multi-threaded environment
            if  key in self.timedict:
                if  self.is_alive(uri) and (time.time()-self.timedict[key]) < self.thr:
                    return self.conndict[key]
                else: # otherwise clean-up
                    del self.timedict[key]
                    del self.conndict[key]
        except:
            pass
        return self.get_new_connection(uri)

    def get_new_connection(self, uri):
        "Get new MongoDB connection"
        key = self.genkey(uri)
        for idx in range(0, self.retry):
            try:
                dbinst = MongoClient(host=uri, **self.mongo_opts)
                self.conndict[key] = dbinst
                self.timedict[key] = time.time()
                return dbinst
            except (ConnectionFailure, AutoReconnect) as exc:
                print("### MongoDB connection failure time=%s", time.time())
                traceback.print_exc()
            except Exception as exc:
                traceback.print_exc()
            time.sleep(idx)
        return self.conndict.get(key, None)

    def is_alive(self, uri):
        "Check if DB connection is alive"
        key = self.genkey(uri)
        if  key in self.conndict:
            conn = self.conndict[key]
            return conn.alive()
        return False

DB_CONN_SINGLETON = DBConnection()

def db_connection(uri, singleton=True):
    """Return DB connection instance"""
    if  singleton:
        dbinst = DB_CONN_SINGLETON.connection(uri)
    else:
        dbinst = DBConnection(pool_size=10).connection(uri)
    return dbinst
