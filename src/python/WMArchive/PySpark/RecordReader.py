"""
This is example how to write mapper and reducer functions for
WMArchive/Tools/myspark.py tool. User should perform all necessary actions with
given set of records and return back desired results. Here our mapper process
records from avro files and collect results into a single dictionary. The
reducer will collect results from all mappers and return back aggregated
information.
"""

def mapper(records):
    """
    Function to extract necessary information from records during spark
    collect process. It will be called by RDD.collect() object within spark.
    """
    out = []
    tot_cpu = tot_time = rsize = wsize = count = 0
    for rec in records:
        if  not rec:
            continue
        perf = rec['steps']['cmsRun1']['performance']
        cpu = perf['cpu']
        storage = perf['storage']
        tot_cpu += cpu['TotalJobCPU']
        tot_time += cpu['TotalJobTime']
        rsize += storage['readTotalMB']
        wsize += storage['writeTotalMB']
        count += 1
    summary = {'cpu':tot_cpu, 'time':tot_time, 'rsize':rsize, 'wsize':wsize, 'docs':count}
    return summary

def reducer(records, init=0):
    "Simpler reducer which collects all results from RDD.collect() records"
    tot_cpu = tot_time = rsize = wsize = count = init
    for rec in records:
        tot_cpu += rec['cpu']
        tot_time += rec['time']
        rsize += rec['rsize']
        wsize += rec['wsize']
        count += rec['docs']
    summary = {'cpu':tot_cpu, 'time':tot_time, 'rsize':rsize, 'wsize':wsize, 'docs':count}
    return summary
