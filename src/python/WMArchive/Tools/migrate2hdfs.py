#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : dump2hdfs.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Write content from given area into HDFS
"""
# futures
from __future__ import print_function, division

# system modules
import os
import re
import sys
import argparse

# pydoop modules
import pydoop.hdfs as hdfs

# WMArchive modules
from WMArchive.Utils.Regex import PAT_YYYYMMDD, PAT_YYYY, PAT_MM, PAT_DD

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='dump2hdfs')
        self.parser.add_argument("--idir", action="store", \
            dest="idir", default="", help="Source area on local file system")
        self.parser.add_argument("--odir", action="store", \
            dest="odir", default="", help="Destination area on HDFS")
        self.parser.add_argument("--check", action="store_true", \
            dest="check", default=False, help="Perform check if file written correctly")
        self.parser.add_argument("--remove", action="store_true", \
            dest="remove", default=False, help="Remove written file from local file system")
        self.parser.add_argument("--verbose", action="store_true", \
            dest="verbose", default=False, help="Be verbose")

def hdfs_file(odir, name):
    """
    Given HDFS dir and file name create appropriate dir structure on HDFS
    and return full path of the file. We rely on odir/YYYY/MM/DD dir structure.
    """
    tstamp = name.split('/').split('_') # each file is in form YYYYMMDD_HHMM.ext
    if  not PAT_YYYYMMDD.match(tstamp):
        raise Exception("Given file name '%s' does not contain YYYYMMDD stamp" % name)
    year = tstamp[:4]
    if  not PAT_YYYY.match(year):
        raise Exception("Given file name '%s' does not contain YYYY stamp" % name)
    month = tstamp[4:6]
    if  not PAT_MM.match(month):
        raise Exception("Given file name '%s' does not contain MM stamp" % name)
    day = tstamp[6:8]
    if  not PAT_DD.match(day):
        raise Exception("Given file name '%s' does not contain DD stamp" % name)
    if  not hdfs.isdir(odir):
        hdfs.mkdir(odir)
    for subdir in [year, month, day]:
        odir = os.path.join(odir, subdir)
        if  not hdfs.path.isdir(odir):
            hdfs.mkdir(odir)
    return os.path.join(odir, name)

def write(idir, odir, remove, check, verbose):
    "Write files from given input area into HDFS"
    if  not os.path.isdir(idir):
        print("Source area %s does not exists" % idir)
        sys.exit(1)
    if  not hdfs.path.isdir(odir):
        print("Destination area on HDFS %s does not exists" % odir)
        print("Create it first with the following command")
        print("hadoop fs -mkdir %s" % odir)
        sys.exit(1)
    for name in os.listdir(idir):
        fname = os.path.join(idir, name)
        if  not (name.endswith('.avro') or \
            name.endswith('.avro.gz') or \
            name.endswith('.avro.bz2')):
            if  verbose:
                print("Skip %s" % fname)
            continue
        oname = hdfs_file(odir, name)
        if  not hdfs.path.isfile(oname):
            if  verbose:
                print("Migrate %s to %s" % (fname, oname))
            hdfs.put(fname, oname)
            if  check:
                fsize = os.stat(fname).st_size
                osize = hdfs.stat(oname).st_size
                if  fsize != osize:
                    print("Size %s (%s) != %s (%s)" % (fname, fsize, oname, osize))
                    sys.exit(1)
            if  remove:
                os.remove(fname)

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    write(opts.idir, opts.odir, opts.remove, opts.check, opts.verbose)

if __name__ == '__main__':
    main()
