"""
This is example how to write simple aggregator mapper and reducer functions for
WMArchive/Tools/myspark.py tool. It collects information about cpu/time/read/write
sizes of successfull FWJR jobs. Information is structured by agent host/site.
"""

class MapReduce(object):
    def __init__(self, spec=None):
        # spec here is redundant since our mapper and reducer does not use it
        self.spec = spec

    def mapper(self, records):
        """
        Function to extract necessary information from records during spark
        collect process. It will be called by RDD.collect() object within spark.
        """
        out = []
        sdict = {}
        hdict = {}
        for rec in records:
            if  not rec:
                continue
            meta = rec['meta_data']
            if  meta['jobstate'] != 'success':
                continue
            host = meta['host']
            if  host not in hdict.keys():
                hdict[host] = {}
            for step in rec['steps']:
                site = step.get('site', None)
                if  not site:
                    continue
                if  site not in hdict[host].keys():
                    hdict[host] = {site:{'cpu':0, 'time':0, 'rsize':0, 'wsize':0}}
                perf = step['performance']
                cpu = perf['cpu']
                storage = perf['storage']
                if  cpu:
                    if  cpu.get('TotalJobCPU', 0):
                        hdict[host][site]['cpu'] += cpu.get('TotalJobCPU', 0)
                    if  cpu.get('TotalJobTime', 0):
                        hdict[host][site]['time'] += cpu.get('TotalJobTime', 0)
                if  storage:
                    if  storage.get('readTotalMB', 0):
                        hdict[host][site]['rsize'] += storage.get('readTotalMB', 0)
                    if  storage.get('writeTotalMB', 0):
                        hdict[host][site]['wsize'] += storage.get('writeTotalMB', 0)
        return hdict

    def reducer(self, records, init=0):
        "Simpler reducer which collects all results from RDD.collect() records"
        out = {}
        count = 0
        mdict = {'cpu':0, 'time':0, 'rsize':0, 'wsize':0}
        for rec in records:
            for host, hdict in rec.items():
                if  host not in out.keys():
                    out[host] = {}
                _hdict = out[host]
                for site, sdict in hdict.items():
                    if  site not in _hdict.keys():
                        _hdict = {site:mdict}
                        out[host][site] = mdict
                    _sdict = _hdict[site]
                    _cpu = _sdict['cpu'] + sdict['cpu']
                    _time = _sdict['time'] + sdict['time']
                    _rsize = _sdict['rsize'] + sdict['rsize']
                    _wsize = _sdict['wsize'] + sdict['wsize']
                    ndict = {'cpu':_cpu, 'time':_time, 'rsize':_rsize, 'wsize':_wsize}
                    out[host][site].update(ndict)
            count += 1
	# simple printout of summary info
        for host, hdict in out.items():
            print(host)
            for site, sdict in hdict.items():
                print(site)
                for key, val in sdict.items():
                    print('%s: %s' % (key, val))
        return out
