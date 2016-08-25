"""
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

def match_lfn(rec, lfn):
    "Find if record match given lfn/pfn"
    if  isinstance(lfn, list):
        lfns = lfn
    else:
        lfns = [lfn]
    meta = rec.get('meta_data', {})
    merged = meta.get('jobtype', '').lower().startswith('merge')
    for idx, val in enumerate(rec.get('LFNArray', [])):
        for lfn in lfns:
            if  match_value(val, lfn):
                return True
    return False

def extract_output(rec, step_name):
    "Extract output files from a record which has given step"
    lfns = [] # our output
    meta = rec.get('meta_data', {})
    merged = meta.get('jobtype', '').lower().startswith('merge')
    lfn_array = rec.get('LFNArray', [])
    for step in rec.get('steps', []):
        if  merged: # if we're given merged record we extract inputLFNs
            if  step.get('name', '').lower().startswith('cmsrun'):
                for item in step.get('output', []):
                    for lfn_idx in item.get('inputLFNs', []):
                        lfn = lfn_array[lfn_idx]
                        lfns.append(lfn)
        else: # if we're given non-merged record we extract outputLFNs of given step_name
            if  step.get('name', '').lower().startswith(step_name.lower()):
                for item in step.get('output', []):
                    for lfn_idx in item.get('outputLFNs', []):
                        lfn = lfn_array[lfn_idx]
                        lfns.append(lfn)
    return lfns

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
        self.query = ''
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
        self.is_lfn = is_ext(self.query, 'root')
        self.is_log = is_ext(self.query, 'tar.gz')
        self.step_name = 'logArch' if self.is_lfn else 'logCollect'

    def mapper(self, records):
        """
        Function to find a record for a given spec during spark
        collect process. It will be called by RDD.map() object within spark.
        The spec of the class is a JSON query which we'll apply to records.
        """
        for rec in records:
            if  not rec:
                continue
            if  self.is_lfn:
                if  match_lfn(rec, self.query):
                    return rec
            elif self.is_log:
                if  match_log(rec, self.query):
                    return rec

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = []
        nrec = 0
        for rec in records:
            if  not rec:
                continue
            nrec += 1
            data = extract_output(rec, self.step_name)
            for item in data:
                out.append(item)
        if  self.step_name == 'logCollect': # final step we'll return results
            return out
        exts = [r.split('.')[-1] for r in out]
        if  len(set(exts)) > 1: # multiple extensions, we'll return non-root entries
            out = [r for r in out if not r.endswith('root')]
        if  not out:
            return out
        return self.make_spec(out)

    def make_spec(self, data):
        "Make WMArchive spec from provided data"
        spec = {'query': data, 'timerange':self.timerange}
        return dict(spec=spec, fields=self.fields)
