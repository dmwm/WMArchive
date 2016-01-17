#!/usr/bin/env python
#-*- coding: utf-8 -*-
#pylint: disable=
"""
File       : mongo2avro.py
Author     : Valentin Kuznetsov <vkuznet AT gmail dot com>
Description: Mongo -> Avro migration script.
We read data from MongoDB storage and writes avro files in given
output directory. The size of avro files can be controlled by --thr
parameter and it should be tuned wrt Hadoop settings for optimal file
size. By default we use 256MB for avro.bz2 file.
"""
# futures
from __future__ import print_function, division

# system modules
import os
import sys
import time
import shutil
import argparse
import itertools

# try to use psutil for memory monitoring
PSUTIL = False
try:
    import psutil
    PSUTIL = True
except ImportError:
    pass

# WMARchive modules
from WMArchive.Storage.MongoIO import MongoStorage
from WMArchive.Storage.AvroIO import AvroStorage
from WMArchive.Utils.Utils import size_format, tstamp

class OptionParser():
    def __init__(self):
        "User based option parser"
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--mongo", action="store",
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--schema", action="store",
            dest="schema", default="", help="Avro schema file")
        self.parser.add_argument("--odir", action="store",
            dest="odir", default="", help="Avro output area")
        thr = 256*1024*1024 # 256MB
        self.parser.add_argument("--thr", action="store", type=int,
            dest="thr", default=thr,
            help="Avro file size threshold, default %sMB" % thr)
        chunk = 1000
        self.parser.add_argument("--chunk", action="store", type=int,
            dest="chunk", default=chunk,
            help="Chunk size for reading Mongo docs, default %s" % chunk)
        self.parser.add_argument("--verbose", action="store_true",
            dest="verbose", default=False, help="Verbose output")

def gen_file_name(odir):
    "Generate new file name in given odir"
    name = time.strftime("%Y%m%d_%H%M%S.avro.bz2", time.gmtime())
    return os.path.join(odir, name)

def file_name(odir, thr):
    """
    Read content of given dir and either re-use existing file or create a new one
    based on given file size threshold. When file exceed given threshold it is
    moved into migrate area within the same given directory.
    """
    files = [f for f in os.listdir(odir) if os.path.isfile(os.path.join(odir,f))]
    if  not files:
        return gen_file_name(odir)

    files.sort()
    last_file = files[-1]
    fname = os.path.join(odir, last_file)
    size = os.path.getsize(fname)
    if  size < thr:
        return fname

    # file is ready for migration
    mdir = os.path.join(odir, 'migrate')
    try:
        os.mkdir(mdir)
    except OSError:
        pass
    shutil.move(fname, mdir)
    return gen_file_name(odir)

def migrate(muri, odir, avsc, thr, chunk=1000, verbose=False):
    "Write data from MongoDB (muri) to avro file(s) on local file system"
    mstg = MongoStorage(muri)
    auri = avsc if avsc.startswith('avroio:') else 'avroio:%s' % avsc
    astg = AvroStorage(auri)

    # read data from MongoDB, returned mdocs is generator type
    query = {'stype': mstg.stype}
    mdocs = mstg.find(query)

    # loop over provided docs and write them into avro file on local file system
    wmaids = []
    osize = 0
    fsize = 0
    while True:
        fname = file_name(odir, thr)
        ids = astg.file_write(fname, itertools.islice(mdocs, chunk))
        fsize = os.path.getsize(fname)
        if  osize == fsize or not len(ids):
            break
        wmaids += ids
        osize = fsize
        if  verbose:
            if  PSUTIL:
		pid = os.getpid()
		proc = psutil.Process(pid)
		mem = proc.memory_info_ex()
                rss = 'RSS:%s' % size_format(mem.rss)
            else:
                rss = ''
            print(tstamp('mongo2avro'), "%s docs %s %s (%s bytes) %s" \
                    % (len(wmaids), fname, size_format(fsize), fsize, rss))
    print(tstamp('mongo2avro'), "wrote %s docs %s %s (%s bytes)" \
            % (len(wmaids), fname, size_format(fsize), fsize))

    # update status attributes of docs in MongoDB
    spec = {'$set' : {'stype': astg.stype}}
    mstg.update(wmaids, spec)

def main():
    "Main function"
    optmgr  = OptionParser()
    opts = optmgr.parser.parse_args()
    migrate(opts.muri, opts.odir, opts.schema, opts.thr, opts.chunk, opts.verbose)

if __name__ == '__main__':
    main()
