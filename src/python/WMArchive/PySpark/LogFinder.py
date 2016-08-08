"""
This is example how to find CMS FWJR log file from provided LFN/PFN.
The procedure:

### Simple use case

1. user gives LFN/PFN
2. we look-up documents which has non-merged meta_data.jobtype
3. within found document we look-up associated tar.gz file which is a log
   file of the job
4. we start new search where document's meta_data.jobtype=='LogCollect'
   and LFNArray has tar.gz from step #3
5. we extract desired log from steps.logCollect*.outputLFNs index
   by look-up log name from outputLFNs index id

This use case is implemented as map/reduce operation where we use match_root
function as mapper and extract_tarball as reducer. The job is two-step procedure,
first we look-up log tar.gz from given LFN/PFN and then we apply the same
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

def match_targz(rec, ifile):
    "Find if record match given tar.gz file"
    meta = rec.get('meta_data', {})
    if  meta.get('jobtype', None) != 'LogCollect':
        return False
    for idx, val in enumerate(rec.get('LFNArray', [])):
        if  match_value(val, ifile):
            return True
    return False

def match_root(rec, lfn):
    "Find if record match given lfn/pfn"
    meta = rec.get('meta_data', {})
    for idx, val in enumerate(rec.get('LFNArray', [])):
        if  match_value(val, lfn):
            # check that steps part has cmsRun
            for step in rec.get('steps', []):
                # check that steps.name is cmsRun
                if  step.get('name', '').startswith('cmsRun'):
                    # check that given LFN index is present in outputLFNs
                    for item in step.get('output', []):
                        outputLFNs = item.get('outputLFNs', [])
                        if  idx in outputLFNs:
                            return True
    return False

def extract_tarball(rec, step_name):
    "Extract output LFN tar.gz files from a record which has logArch step"
    lfns = []
    lfn_array = rec.get('LFNArray', [])
    for step in rec.get('steps', []):
        if  step.get('name', '').startswith(step_name):
            for item in step.get('output', []):
                for lfn_idx in item.get('outputLFNs', []):
                    lfn = lfn_array[lfn_idx]
                    lfns.append(lfn)
    return lfns

class MapReduce(object):
    def __init__(self, ispec=None):
        self.lfn = None
        if  ispec:
            spec = ispec['spec']
            self.lfn = spec.get('lfn', None)
        if  not self.lfn:
            raise Exception("No input LFN in a spec")

    def mapper(self, records):
        """
        Function to find a record for a given spec during spark
        collect process. It will be called by RDD.map() object within spark.
        The spec of the class is a JSON query which we'll apply to records.
        """
        for rec in records:
            if  not rec:
                continue
            if  self.lfn.endswith('.root'):
                if  match_root(rec, self.lfn):
                    return rec
            elif self.lfn.endswith('.tar.gz'):
                if  match_targz(rec, self.lfn):
                    return rec
        return {}

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = []
        nrec = 0
        recs = []
        for rec in records:
            if  not rec:
                continue
            nrec += 1
            if  self.lfn.endswith('.root'):
                step_name = 'logArch'
            elif self.lfn.endswith('.tar.gz'):
                step_name = 'logCollect'
            lfns = extract_tarball(rec, step_name)
            for lfn in lfns:
                out.append(lfn)
            recs.append(rec)
        return {"nrecords":nrec, "logFiles": out}
