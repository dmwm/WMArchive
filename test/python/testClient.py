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

def client(host, port, jsonFile):
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
    for idx in range(10):
        rec = dict(record)
        rec['_rev'] = str(idx)*32
        docs.append(rec)
    data = dict(data=docs)
    conn.request('POST', path, json.dumps(data), headers)
    data = getData(conn)
    print("Posted", data)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    client(opts.host, opts.port, opts.json)

if __name__ == '__main__':
    main()
