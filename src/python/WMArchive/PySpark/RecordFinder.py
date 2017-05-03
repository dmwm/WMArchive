#-*- coding: ISO-8859-1 -*-
# Author: Valentin Kuznetsov <vkuznet AT gmail dot com>
"""
MapReduce record finder module.
User must supply valid spec with FWJR parameter, e.g.
{"spec":{"task":"/amaltaro_StepChain_ReDigi3_HG1612_WMArchive_161130_192654_9283/DIGI","timerange":[20161130,20161202]}, "fields":[]}
"""

import re
import json

def write_records(fname, records):
    "Write records to given file name"
    count = 0
    with open(fname, 'w') as ostream:
        ostream.write('[\n')
        for rec in records:
            if  count:
                ostream.write(",\n")
            ostream.write(json.dumps(rec))
            count += 1
        ostream.write("]\n")

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

def match_cmsrun(rec, key, val):
    "Match key, val pair in cmsRun portion of the record"
    for step in rec['steps']:
        if  step.get('name', '').lower().startswith('cmsrun'):
            for output in step['output']:
                for kkk, vvv in output.items():
                    if  kkk.lower() == key:
                        if  isinstance(vvv, list):
                            for value in vvv:
                                if  match_value(str(value), val):
                                    return True
                        else:
                            if  match_value(str(vvv), val):
                                return True

def match_topkey(rec, key, val):
    "Match key,val pair in top level keys of the record"
    if key == 'lfn':
        for lfn in rec['LFNArray']:
            if match_value(lfn, val):
                return True
    elif key in rec:
        return match_value(rec[key], val)
    return False

def match(rec, spec):
    "Find if record match given spec"
    for key, val in spec.items():
        if  key in rec.keys(): # top level keys such as task
            if  match_topkey(rec, key, val):
                return True
        if key == 'lfn':
            for lfn in rec['LFNArray']:
                if  match_value(lfn, val):
                    return True
        if  match_cmsrun(rec, key, val):
            return True
    return False

class MapReduce(object):
    def __init__(self, ispec=None):
        self.name = __file__.split('/')[-1]
        self.fields = []
        self.verbose = ispec.get('verbose', None)
        self.output = ispec.get('output', '')
        if  self.verbose:
            del ispec['verbose']
        if  self.output:
            del ispec['output']
        if  ispec:
            if  'spec' in ispec:
                self.spec = ispec['spec']
            if  'fields' in ispec:
                self.fields = ispec['fields']
            if  'timerange' in ispec:
                del ispec['timerange'] # this is not used for record search
            self.spec = parse_spec(self.spec)
        else:
            self.spec = {}
        if  self.verbose:
            print("### SPEC", self.spec)

    def mapper(self, pair):
        """
        Function to filter given pair from RDD, see myspark.py
        """
        rec, _ = pair # we receive a pair (record, key) from RDD
        return match(rec, self.spec)

    def reducer(self, records):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = []
        nrec = 0
        if  self.verbose:
            print("### Mapper found %s matches" % len(records))
        for rec in records:
            if  rec:
                if  isinstance(rec, tuple):
                    rec = rec[0] # take record from (rec,key) pair
                nrec += 1
                if  self.fields:
                    fields = [rec[f] for f in self.fields]
                    out.append(fields)
                else:
                    out.append(rec)
        if  self.output:
            write_records(self.output, out)
        return out
