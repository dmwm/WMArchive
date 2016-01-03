#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongo2hdfs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Mongo -> HDFS migration script. It puts data from MongoDB into HDFS.
"""
# futures
from __future__ import print_function, division

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
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--mongo", action="store",
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--hdfs", action="store",
            dest="huri", default="", help="HDFS URI")

def migrate(muri, huri):
    "Migrate data from MongoDB (muri) to HDFS (huri)"
    mstg = MongoStorage(muri)
    hstg = HdfsStorage(huri)

    # read data from MongoDB
    query = {'stype': mstg.stype}
    mdocs = mstg.read(query)
    mids = [d['wmaid'] for d in mdocs]

    # do nothing if no documents is found
    if  not len(mdocs):
        return

    # store data to HDFS
    wmaid = hstg.write(mdocs)

    # read data from HDFS
    hdocs = hstg.read(wmaid)

    # now we can compare MongoDB docs with HDFS docs, a la cross-check
    for mdoc, hdoc in zip(mdocs, hdocs):
        # drop WMArchive keys
        for key in ['stype', 'wmaid']:
            if  key in mdoc:
                del mdoc[key]
            if  key in hdoc:
                del hdoc[key]
        if mdoc != hdoc:
            print("ERROR", mdoc, hdoc)
            sys.exit(1)

    # update status attributes of docs in MongoDB
    query = {'$set' : {'stype': hstg.stype}}
    mstg.update(mids, query)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.muri, opts.huri)

if __name__ == '__main__':
    main()
