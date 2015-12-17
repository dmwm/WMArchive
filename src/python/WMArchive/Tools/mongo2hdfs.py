#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongo2hdfs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: 
"""

# system modules
import os
import sys
import argparse

# WMARchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Storage.HdfsIO import HdfsStorage

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='PROG')
        self.parser.add_argument("--mongo", action="store",
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--hdfs", action="store",
            dest="huri", default="", help="HDFS URI")

def migrate(muri, huri):
    "Migrate data from MongoDB (muri) to HDFS (huri)"
    mstg = MongoStorage(muri)
    hstg = HdfsStorage(huri)

    # read data from MongoDB
    query = {'status': 'mongo'}
    mdocs = mstg.read(query)

    # do nothing if no documents is found
    if  not len(mdocs):
        return

    # store data to HDFS
    wmaid = hstg.write(mdocs)

    # read data from HDFS
    hdocs = hstg.read(wmaid)

    # now we can compare MongoDB docs with HDFS docs, a la cross-check
    for mdoc, hdoc in zip(mdocs, hdocs):
        # drop statuses for time being
        del mdoc['status']
        del hdoc['status']
        if mdoc != hdoc:
            print "ERROR", mdoc, hdoc
            sys.exit(1)

    # update status attributes of docs in MongoDB
    ids = [d['wmaid'] for d in mdocs]
    query = {'$set' : {'status': 'hdfs'}}
    mstg.update(ids, query)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.muri, opts.huri)

if __name__ == '__main__':
    main()
