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
from WMArchive.Service.STS import STSManager
try:
    from WMArchive.Service.LTS import LTSManager
    LTS = True
except Exception as exp:
    print("WARNING, unable to load LTS", str(exp))
    LTS = False
from WMArchive.Utils.Utils import wmaHash, tstamp, check_tstamp, dateformat
from WMArchive.Utils.Exceptions import WriteError, ReadError

def trange_check(trange):
    "Check correctnes of trange values"
    return len(trange) == 2 & check_tstamp(trange[0]) & check_tstamp(trange[1])

def use_lts(trange, thr):
    """
    Helper function to determine based on given time range either
    to use Short Term Storage or Long Term Storage
    """
    # check if max time is less than given threshold
    maxt = dateformat(trange[1])
    if  time.time()-maxt > thr:
        return True
    return False

class WMArchiveManager(object):
    """
    Initialize WMArchive proxy server configuration. The given configuration
    file will provide details of proxy server, agent information, etc.
    """
    def __init__(self, config=None):
        # Short-Term Storage
        self.sts = STSManager(config.short_storage_uri)
        # Long-Term Storage
        self.tls_thr = config.long_storage_thr
        if  LTS: # we'll use this module if it's loaded
            self.lts = LTSManager(config.long_storage_uri, config.wmauri, config.yarn)
        else: # fallback
            self.lts = self.sts
        self.specmap = {}
        with open(config.specmap, 'r') as istream:
            cdict = {}
            for line in istream.readlines():
                pair = line.replace('\n', '').split(',')
                self.specmap[pair[0]] = pair[1] # lfn:LFNArray
        msg = "Short-Term Storage %s, Long-Term Storage %s, specmap %s" % (self.sts, self.lts, self.specmap)
        print(tstamp("WMArchiveManager::init"), msg)

    def qmap(self, mgr, spec, fields):
        "Map user based spec into WMArhchive storage QL"
        newspec = {}
        newfields = []
        for key, val in spec.items():
            newspec[self.specmap.get(key, key)] = val
        for field in fields:
            newfields.append(self.specmap.get(field, field))
        if  hasattr(mgr, 'qmap'):
            return mgr.qmap(newspec, fields)
        return newspec, newfields

    def encode(self, docs):
        """
        Encode given set of documents into appropriate format for long term storage.
        This method will consume documents in DMWM JSON format.
        Yield encoded documents to the client.
        """
        for doc in docs:
            if  not 'wmaid' in doc:
                doc['wmaid'] = wmaHash(doc)
            if  not 'wmats' in doc:
                doc['wmats'] = time.time()
            if  not 'stype' in doc:
                doc['stype'] = self.sts.stype
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
            ids = self.sts.write(docs)
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
        result = {'stype': self.sts.stype, 'ids': ids, 'status': status}
        return result

    def read(self, spec, fields):
        """
        Send request to proxy server to read data for given query.
        Yield list of found documents or None.
        """
        result = {'input': {'spec': spec, 'fields': fields},
                  'results': [], 'storage': self.sts.stype, 'status': 'ok'}
        # convert given spec into query suitable for sts/lts
        if  isinstance(spec, dict):
            try:
                trange = spec.pop('timerange')
            except KeyError:
                print(tstamp("WMArchiveManager::read"), "timerange is not provided")
                result['reason'] = 'No timerange is provided, please adjust your query spec'
                result['status'] = 'fail'
                return result

            if  trange_check(trange):
                print(tstamp("WMArchiveManager::read"), "bad timerange: %s" % trange)
                result['reason'] = 'Unable to parse timerange, should be [YYYYMMDD, YYYYMMDD]'
                result['status'] = 'fail'
                return result

            # based on given time range define which manager
            # we'll use for data look-up
            mgr = self.sts
            if  use_lts(trange, self.tls_thr):
                spec['timerange'] = trange # put back timerange for HDFS hdir constraint
                mgr = self.lts

            # convert spec into WMArchive one
            spec, fields = self.qmap(mgr, spec, fields)
        else:
            # if spec is a list, it means user look-up docs by wmaids
            # they represents results of LTS data look-up
            mgr = self.sts
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
        result['data'] = data
        result['status'] = status
        if  reason:
            result['reason'] = reason
        return result
