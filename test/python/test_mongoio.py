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

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='test_mongo')
        self.parser.add_argument("--mongo", action="store",
                dest="muri", default="mongodb://localhost:8230", help="MongoDB URI")

def migrate(muri):
    "Write and read data to MongoDB"
    mstg = MongoStorage(muri, dbname='test_fwjr')
    doc = {"test":1, 'wmaid':1}
    mstg.write([doc])

    # read data from MongoDB
    query = {}
    mdocs = mstg.read(query)
    for doc in mdocs:
        print(doc)
        if '_id' in doc:
            print("found _id in doc")
            print(doc)
            break

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.muri)

if __name__ == '__main__':
    main()
