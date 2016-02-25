"""
This is example how to write mapper and reducer functions for
WMArchive/Tools/myspark.py tool. User should perform all necessary actions with
given set of records and return back desired results. Here our mapper process
records from avro files and collect results into a single dictionary. The
reducer will collect results from all mappers and return back aggregated
information.
"""

SPEC = {}

def mapper(records):
    """
    Function to extract necessary information from records during spark
    collect process. It will be called by RDD.map() object within spark.
    The given spec is JSON query to apply to records
    """
    out = []
    for rec in records:
        if  not rec:
            continue
        for key, val in SPEC.items():
            if key in rec:
                if hasattr(val, 'pattern'): # it is re.compile pattern
                    if val.match(rec[key]):
                        out.append(rec)
                else:
                    if rec[key] == val:
                        out.append(rec)
    return out

def reducer(records, init=0):
    "Simpler reducer which collects all results from RDD.collect() records"
    return records
