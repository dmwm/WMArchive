#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongocleanup.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Mongo clean up script
"""
# futures
from __future__ import print_function, division

# system modules
import argparse

# WMARchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Utils.Utils import dateformat

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mongocleanup')
        self.parser.add_argument("--mongo", action="store",\
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--tstamp", action="store",\
            dest="tstamp", default="",\
            help="timestamp below which records will be removed, YYYYMMDD \
            or number with suffix 'd' for days")
        self.parser.add_argument("--verbose", action="store_true",\
            dest="verbose", default=False, help="verbose mode")

def cleanup(muri, tst, verbose):
    "Cleanup data in MongoDB (muri) for given timestamp (tst)"
    mstg = MongoStorage(muri)
    # remove records whose type is hdfsio, i.e. already migrated to HDFS,
    # and whose time stamp is less than provided one
    query = {'stype': 'hdfsio', 'time_stamp':{'$lt': dateformat(tst)}}
    response = mstg.remove(query)
    if  verbose:
        print("uri :", muri)
        print("spec:", query)
        print("response:", response)

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    cleanup(opts.muri, opts.tstamp, opts.verbose)

if __name__ == '__main__':
    main()
