"""
This is example how to write mapper and reducer methods of MapReduce class for
WMArchive/Tools/myspark.py tool. User should perform all necessary actions with
given set of records and return back desired results. Here our mapper process
records from avro files and collect results into a single dictionary. The
reducer will collect results from all mappers and return back aggregated
information.
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

def match(rec, spec):
    "Find if record match given spec"
    for key, val in spec.items():
        if key in rec:
            if hasattr(val, 'pattern'): # it is re.compile pattern
                if val.match(rec[key]):
                    return True
            else:
                if rec[key] == val:
                    return True
    return False

class MapReduce(object):
    def __init__(self, spec=None):
        if  spec:
            self.spec = parse_spec(spec)
        else:
            self.spec = {}

    def mapper(self, records):
        """
        Function to find a record for a given spec during spark
        collect process. It will be called by RDD.map() object within spark.
        The spec of the class is a JSON query which we'll apply to records.
        """
        for rec in records:
            if  not rec:
                continue
            if  match(rec, self.spec):
                return rec
        return {}

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = []
        nrec = 0
        for rec in records:
            if  rec:
                nrec += 1
                out.append(rec)
        return {"nrecords":nrec, "result":out}
