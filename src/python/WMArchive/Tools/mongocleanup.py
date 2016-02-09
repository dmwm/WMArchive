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
import time
import argparse

# WMARchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Utils.Utils import dateformat, elapsed_time

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
        self.parser.add_argument("--rtype", action="store",\
            dest="rtype", default="avroio", help="Record type to clean-up, default avroio")
        self.parser.add_argument("--verbose", action="store_true",\
            dest="verbose", default=False, help="verbose mode")

def cleanup(muri, tst, rtype, verbose):
    "Cleanup data in MongoDB (muri) for given timestamp (tst)"
    time0 = time.time()
    mstg = MongoStorage(muri)
    # remove records whose type is hdfsio, i.e. already migrated to HDFS,
    # and whose time stamp is less than provided one
    query = {'stype': rtype, 'time_stamp':{'$lt': dateformat(tst)}}
    if  verbose:
        print("Clean-up records in MongoDB: %s" % muri)
        print("MongoDB cleanup spec:", query)
    response = mstg.remove(query)
    if  verbose:
        print("response: %s" % response)
        print("Elapsed time: %s" % elapsed_time(time0))

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    cleanup(opts.muri, opts.tstamp, opts.rtype, opts.verbose)

if __name__ == '__main__':
    main()
