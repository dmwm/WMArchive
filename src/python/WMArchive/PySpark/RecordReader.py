"""
This is example how to write extract function to be used by
WMArchive/Tools/myspark.py tool. User should perform all
necessary actions with given set of records and return back
desired results.
"""

def extract(records):
    """
    Function to extract necessary information from record during spark
    collect process. It will be called by RDD.collect() object within spark.
    """
    out = []
    for rec in records:
        # do something useful with records, so far we just
        # put them into output, input records parameters is
        # what spark will extract from avro files and pass
        # to this function.
        out.append(rec)
    return out


