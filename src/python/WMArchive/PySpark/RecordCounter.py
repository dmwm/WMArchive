#-*- coding: ISO-8859-1 -*-
# Author: Valentin Kuznetsov <vkuznet AT gmail dot com>
"""
This is simple record counting module for WMArchive/Tools/myspark.py tool.
User must supply valid spec file, e.g.
{"spec":{"timerange":[20160601,20161203]}, "fields":[]}
"""

import re

def parse_spec(spec):
    "Simple spec parser, it converts strings to patterns so far"
    ospec = {}
    for key, val in spec.items():
        if  isinstance(val, basestring):
            ospec[key] = re.compile(val)
        else:
            ospec[key] = val
    return ospec

def match_value(keyval, value):
    "helper function to match value from spec with keyval"
    if hasattr(value, 'pattern'): # it is re.compile pattern
        if value.match(keyval):
            return True
    else:
        if keyval == value:
            return True
    return False

def match(rec, spec):
    "Find if record match given spec"
    if  not spec:
        return True
    for key, val in spec.items():
        if key == 'lfn':
            for lfn in rec['LFNArray']:
                if match_value(lfn, val):
                    return True
        elif key in rec:
            return match_value(rec[key], val)
    return False

class MapReduce(object):
    def __init__(self, ispec=None):
        self.name = __file__.split('/')[-1]
        self.fields = []
        if  ispec:
            if  'spec' in ispec:
                self.spec = ispec['spec']
            if  'fields' in ispec:
                self.fields = ispec['fields']
            if  'timerange' in self.spec:
                del self.spec['timerange'] # this is not used for record search
            self.spec = parse_spec(self.spec)
        else:
            self.spec = {}

    def mapper(self, pair):
        """
        Function to filter given pair, see myspark.py
        """
        rec, _ = pair # we receive (record, key) from RDD
        return match(rec, self.spec)

    def reducer(self, records):
        "Simpler reducer which collects all results from RDD.collect() records"
        nrec = 0
        for rec in records:
            if  not rec:
                continue
            nrec += 1
        return {"nrecords":nrec}
