#-*- coding: ISO-8859-1 -*-
# Author: Valentin Kuznetsov <vkuznet AT gmail dot com>
"""
This MapReduce module aggregates records for given site parameter.
Used must supply valid spec with valid site attribute, e.g.
{"spec":{"site":"T2_CH_CERN", "timerange":[20161205,20161205]}, "fields":[]}
"""

import re

class MapReduce(object):
    def __init__(self, ispec=None):
        self.name = __file__.split('/')[-1]
        self.fields = []
        if  ispec:
            if  'spec' in ispec:
                self.spec = ispec['spec']
            if  'fields' in ispec:
                self.fields = ispec['fields']
            if  'timerange' in ispec:
                del ispec['timerange'] # this is not used for record search
        else:
            self.spec = {}
        self.site = self.spec.get('site', 'T2_CH_CERN')
        if  not isinstance(self.site, list):
            self.site = [self.site] # make it a list
        self.attrs = ['readTotalMB', 'readMBSec', 'readAveragekB', 'readMaxMSec',
                 'readTotalSecs', 'writeTotalSecs', 'writeTotalMB']
        self.counters = {}
        for attr in self.attrs:
            self.counters[attr] = {'count0':0, 'count1':0, 'mean':0, 'total':0, 'min':0, 'max':0}

    def mapper(self, pair):
        """
        Function to find a record for a given spec during spark
        collect process. It will be called by RDD.map() object within spark.
        The spec of the class is a JSON query which we'll apply to records.
        """
        rec, _ = pair # we get RDD (record, key) pairs
        meta = rec.get('meta_data', {})
        if meta['jobstate'] == "success":
            for step in rec.get('steps', []):
                if  step.get('site', '') not in self.site: # self.site is a list of sites
                    continue
                if step['name'].lower().startswith('cmsrun'):
                    return True
        return False

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        count = 0
        nrec = 0
        maxRecord = None
        minRecord = None
        for rec in records:
            if  rec:
                if isinstance(rec, tuple):
                    rec = rec[0] # take record from the pair
                meta = rec.get('meta_data', {})
                for step in rec.get('steps', []):
                    if step['name'].lower().startswith('cmsrun'):
                        perf = step.get('performance', {})
                        storage = perf.get('storage', {})
                        break
                for attr in self.attrs:
                    tot = storage.get(attr, None)
                    if tot == None:
                        self.counters[attr]['count1'] += 1
                    elif tot == 0:
                        self.counters[attr]['count0'] += 1
                    if  tot != None:
                        self.counters[attr].setdefault('records', []).append(tot)
                        self.counters[attr]['total'] += 1
                        if (self.counters[attr]['max'] < tot):
                            self.counters[attr]['max']  = tot
                            if  attr == 'writeTotalSecs':
                                maxRecord = meta
                        if (self.counters[attr]['min'] > tot or self.counters[attr]['min'] == 0):
                            self.counters[attr]['min']  = tot
                            if  attr == 'writeTotalSecs':
                                minRecord = meta
            nrec += 1
        for attr in self.attrs:
            if  'records' in self.counters[attr].keys():
                self.counters[attr]['mean'] = \
                        sum(self.counters[attr]['records'])/len(self.counters[attr]['records'])
                del self.counters[attr]['records']
            else:
                self.counters[attr]['mean'] = 'NA'
        self.counters['nrecords'] = nrec
        self.counters['count'] = count
        self.counters['minRecord'] = minRecord
        self.counters['maxRecord'] = maxRecord
        return self.counters
