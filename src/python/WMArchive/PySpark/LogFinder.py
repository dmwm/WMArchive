#-*- coding: ISO-8859-1 -*-
"""
File       : LogFinder.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>

This is example how to find CMS FWJR log file from provided LFN.
The procedure:

### Simple use case

1. user gives LFN
2. we look-up documents which has non-merged meta_data.jobtype
3. within found document we look-up associated tar.gz file which is a log
   file of the job
4. we start new search where document's meta_data.jobtype=='LogCollect'
   and LFNArray has tar.gz from step #3
5. we extract desired log from steps.logCollect*.outputLFNs index
   by look-up log name from outputLFNs index id

This use case is implemented as map/reduce operation where we use match_root
function as mapper and extract_output as reducer. The job is two-step procedure,
first we look-up log tar.gz from given LFN and then we apply the same
procedure to look-up desired log (on SRM) from given log tar.gz input.

### Merge use case

1. user gives us an LFN, it is served as outputLFN for doc look-up
2. find inputLFN from found doc
3. perform simple use case

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

def match_value(keyval, value):
    "helper function to match value from spec with keyval"
    if hasattr(value, 'pattern'): # it is re.compile pattern
        if value.match(keyval):
            return True
    else:
        if keyval == value:
            return True
    return False

def match_log(rec, ifile):
    "Find if record match given log file(s)"
    meta = rec.get('meta_data', {})
    if  meta.get('jobtype', '').lower() != 'logcollect':
        return False
    if  isinstance(ifile, list):
        logs = ifile
    else:
        logs = [ifile]
    for idx, val in enumerate(rec.get('LFNArray', [])):
        for log in logs:
            if  match_value(val, log):
                return True
    return False

def match_lfn(rec, lfn, is_output=False):
    "Find if record match given lfn/pfn"
    if  isinstance(lfn, list):
        lfns = lfn
    else:
        lfns = [lfn]
    meta = rec.get('meta_data', {})
    merged = meta.get('jobtype', '').lower().startswith('merge')
    # get outputLFNs ids
    if  is_output:
        oids = set()
        for step in rec.get('steps', []):
            for item in step.get('output', []):
                for lfn_idx in item.get('outputLFNs', []):
                    oids.add(lfn_idx)
        lfn_array = rec.get('LFNArray', [])
        for oid in oids:
            fname = lfn_array[oid]
            if  fname in lfns:
                return True
    else:
        for lfn_idx, val in enumerate(rec.get('LFNArray', [])):
            for lfn in lfns:
                if  match_value(val, lfn):
                    return True
    return False

def extract_output(rec, step_name):
    "Extract output files from a record which has given step"
    lfns = set() # our output
    meta = rec.get('meta_data', {})
    merged = meta.get('jobtype', '').lower().startswith('merge')
    lfn_array = rec.get('LFNArray', [])
    for step in rec.get('steps', []):
        if  merged: # if we're given merged record we extract inputLFNs
            if  step.get('name', '').lower().startswith('cmsrun'):
                for item in step.get('output', []):
                    for lfn_idx in item.get('inputLFNs', []):
                        lfn = lfn_array[lfn_idx]
                        lfns.add(lfn)
        else: # if we're given non-merged record we extract outputLFNs of given step_name
            if  step.get('name', '').lower().startswith(step_name.lower()):
                for item in step.get('output', []):
                    for lfn_idx in item.get('outputLFNs', []):
                        lfn = lfn_array[lfn_idx]
                        lfns.add(lfn)
    return list(lfns)

def is_ext(uinput, ext):
    """
    Helper function to check consistency of user input extensions, e.g. if all
    given files in user input has the same extension (root files)
    """
    is_out = False
    if  isinstance(uinput, basestring):
        is_out = uinput.endswith(ext)
    elif isinstance(uinput, list):
        for idx, val in enumerate(uinput):
            if  not idx:
                is_out = val.endswith(ext)
                continue
            is_out *= val.endswith(ext)
    return is_out

class MapReduce(object):
    def __init__(self, ispec=None):
        self.name = __file__.split('/')[-1]
        self.ispec = ispec
        self.query = ''
        self.verbose = ispec.get('verbose', False) if ispec else False
        self.output = ispec.get('output', '') if ispec else ''
        if  self.verbose:
            print("### ispec", ispec)
        if  self.output:
            del ispec['output']
        if  ispec:
            spec = ispec['spec']
            self.fields = ispec.get('fields', [])
            self.timerange = spec.get('timerange', [])
            self.query = spec.get('lfn', '')
            if  not self.query:
                self.query = spec.get('log', '') # user may provide tar.gz log file
            if  not self.query:
                self.query = spec.get('query', '')
        else:
            raise Exception("No spec is provided")
        if  not self.query:
            raise Exception("No input query is provided in a spec")
        self.is_output = False
        if  'queries' in self.ispec: # second phase look-up
            self.is_output = True
        self.is_lfn = is_ext(self.query, 'root')
        self.is_log = is_ext(self.query, 'tar.gz')
        if  not self.is_log:
            self.is_log = is_ext(self.query, 'tar')
        self.step_name = 'logArch' if self.is_lfn else 'logCollect'
        if  self.verbose:
            print("### query", self.query)
            print("### is_output", self.is_output, self.step_name, "is_log", self.is_log)

    def mapper(self, pair):
        """
        Function to filter given pair from RDD, see myspark.py
        """
        rec, _ = pair # we receive a pair (record, key) from RDD
        if  self.is_lfn:
            return match_lfn(rec, self.query, self.is_output)
        elif self.is_log:
            return match_log(rec, self.query)
        return False

    def reducer(self, records):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = []
        nrec = 0
        if  self.verbose:
            print("### reducer", len(records))
        for rec in records:
            if  not rec:
                continue
            if  isinstance(rec, tuple):
                rec = rec[0]
            nrec += 1
            data = extract_output(rec, self.step_name)
            for item in data:
                out.append(item)
        if  self.verbose:
            print("### matches", nrec, " reducer", len(out))
        if  self.step_name == 'logCollect': # final step we'll return results
            odict = {'logCollect': out}
            queries = self.ispec.get('queries', [])
            if  queries:
                odict.update({'queries':queries})
            if  self.output:
                write_records(self.output, [odict])
                return
            return json.dumps(odict)
        exts = [r.split('.')[-1] for r in out]
        if  len(set(exts)) > 1: # multiple extensions, we'll return non-root entries
            out = [r for r in out if not r.endswith('root')]
        return self.make_spec(out)

    def make_spec(self, data):
        "Make WMArchive spec from provided data"
        spec = {'query': data, 'timerange':self.timerange}
        sdict = dict(spec=spec, fields=self.fields)
        if  self.verbose:
            sdict['verbose'] = self.verbose
        if  self.output:
            sdict['output'] = self.output
        queries = self.ispec.get('queries', [])
        if  isinstance(data, list):
            for item in data:
                if  item not in queries:
                    queries.append(item)
        else:
            queries.append(data)
        sdict['queries'] = queries
        return sdict
