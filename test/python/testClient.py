#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : testClient.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description:
"""
# futures
from __future__ import print_function, division

# system modules
import os
import json
import random
import httplib
import argparse

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--host", action="store",
            dest="host", default="localhost", help="Host name, default localhost")
        self.parser.add_argument("--port", action="store",
            dest="port", default=8247, help="Port number, default 8247")
        self.parser.add_argument("--json", action="store",
            dest="json", default="", help="Input json file")
        self.parser.add_argument("--ntimes", action="store",
            dest="ntimes", default="", help="Specify how many copies you need")

def getData(conn):
    "Fetch data from conn"
    response = conn.getresponse()
    print("STATUS", response.status, "REASON", response.reason)
    res = response.read()
    try:
        data = json.loads(res)
        print("data", data, type(data))
        return data
    except:
        print("response", res, type(res))
        raise

def modjson(rec):
    "Change json record"
    if 'steps' not in rec:
        rec['steps'] = {}
    if 'cmsRun1' not in rec['steps']:
        rec['steps']['cmsRun1'] = {}
    if 'performance' not in rec['steps']['cmsRun1']:
        rec['steps']['cmsRun1']['performance'] = {}
    cpu = dict(AvgEventCPU=random.random(),
            AvgEventTime=random.random(),
            MaxEventCPU=random.randint(1,10)*random.random(),
            MaxEventTime=random.randint(1,10)*random.random(),
            MinEventTime=random.random(),
            TotalEventCPU=random.randint(10,1000)*random.random(),
            TotalJobCPU=random.randint(10,1000)*random.random())
    memory = dict(PeakValueRss=random.randint(1,100)*random.random(),
            PeakValueVsize=random.randint(1,100)*random.random())
    storage = {}
    storage = {'readAveragekB': random.randint(10,1000)*random.random(),
      'readCachePercentageOps': random.random(),
      'readMBSec': random.random(),
      'readMaxMSec': random.randint(1,1000)*random.random(),
      'readNumOps': random.randint(1,100)*random.random(),
      'readPercentageOps': random.randint(1,10)*random.random(),
      'readTotalMB': random.randint(1,1000)*random.random(),
      'readTotalSecs': random.random(),
      'writeTotalMB': random.randint(1,1000)*random.random(),
      'writeTotalSecs': random.randint(1000,100000)*random.random()}
    rec['steps']['cmsRun1']['performance'] = \
            dict(cpu=cpu, memory=memory, multicore={}, storage=storage)
    return rec

def client(host, port, jsonFile, ntimes=10):
    "Client program"

    conn = httplib.HTTPConnection(host, port)

    # post improper data to archive
#    params = dict(data=json.load(open(jsonFile)))
#    params['non_existing_attr'] = 'bla'
#    headers = {'Content-type': 'application/json', 'Accept':'application/json'}
#    path = '/wmarchive/data'
#    conn.request('POST', path, json.dumps(params), headers)
#    data = getData(conn)

    # post some valid data to archive
#    params = dict(data=json.load(open(jsonFile)))
#    headers = {'Content-type': 'application/json', 'Accept':'application/json'}
#    path = '/wmarchive/data'
#    conn.request('POST', path, json.dumps(params), headers)
#    data = getData(conn)

    # get data from archive
#    for row in data['result']:
#        rec = json.loads(row)
#        for uid in rec['ids']:
#            path = '/wmarchive/data/%s' % uid
#            print("path", path)
#            conn.request('GET', path, headers=headers)
#            data = getData(conn)

    # post series docs to WMArchive
    record = json.load(open(jsonFile))
    headers = {'Content-type': 'application/json', 'Accept':'application/json'}
    path = '/wmarchive/data'
    docs = []
    for idx in range(ntimes):
        rec = dict(record)
        rec['copyid'] = str(idx)
        docs.append(rec)
    print("Created %s copies" % len(docs))
    data = dict(data=docs)
    conn.request('POST', path, json.dumps(data), headers)
    data = getData(conn)
    for row in data["result"]:
        rec = json.loads(row)
        print("Posted", len(rec["ids"]))

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    client(opts.host, opts.port, opts.json, int(opts.ntimes))

if __name__ == '__main__':
    main()
