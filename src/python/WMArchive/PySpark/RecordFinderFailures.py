#-*- coding: ISO-8859-1 -*-
"""
File       : RecordAggregator.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>

This is example of finding failures (exit codes) in FWJR for
WMArchive/Tools/myspark.py tool.
"""

import re
import sys
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

def match(rec, spec):
    "Find if record match given spec"
    meta = rec['meta_data']
    if  meta['jobstate'] != 'jobfailed':
        return False
    for key, val in spec.items():
        key = key.lower()
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
    return False

def valid_exitCode(exitCode):
    "Check if exit code is valid"
    return exitCode not in (None, 'None', 99999, '99999')

def select(rec):
    "Return only selected information from the record"
    out = {}
    # select meta-info
    out['jobtype'] = rec['meta_data']['jobtype']
    out['workflow'] = rec['task'].split('/')[1]

    # extract steps info
    site = None
    exitCode = None
    exitStep = None
    for step in rec['steps']:
        if site is None:
            site = step.get('site')
        if  not valid_exitCode(exitCode):
            for error in step['errors']:
                exitCode = error.get('exitCode', None)
                exitStep = step['name']
                if  valid_exitCode(exitCode):
                    break
        if  valid_exitCode(exitCode):
            break
    out['exitCode'] = exitCode
    out['exitStep'] = exitStep
    out['site'] = site
    return out
        
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
        rec, _ = pair # we receive a pair of (record, key) from RDD
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
                        rec = rec[0] # take record from the pair
                    nrec += 1
                    out.append(select(rec))
        if  self.output:
            write_records(self.output, out)
        return {"nrecords":nrec}

def test(fname):
    records = json.load(open(fname))
    for rec in records:
        out = select(rec)
        print(out)

if __name__ == '__main__':
    test(sys.argv[1])
