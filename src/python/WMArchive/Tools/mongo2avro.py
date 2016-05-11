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
import time
import shutil
import argparse
import itertools
import threading

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
from WMArchive.Utils.Utils import dateformat, elapsed_time

class OptionParser(object):
    "User based option parser"
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='mongo2hdfs')
        self.parser.add_argument("--mongo", action="store",\
            dest="muri", default="", help="MongoDB URI")
        self.parser.add_argument("--schema", action="store",\
            dest="schema", default="", help="Avro schema file")
        self.parser.add_argument("--odir", action="store",\
            dest="odir", default="", help="Avro output area")
        self.parser.add_argument("--mdir", action="store",\
            dest="mdir", default="", help="Avro migration area")
        self.parser.add_argument("--compress", action="store",\
            dest="compress", default="", help="Use compression, gz or bz2 are supported")
        thr = 256 # 256MB
        self.parser.add_argument("--thr", action="store", type=int,\
            dest="thr", default=thr,\
            help="Avro file size threshold in MB, default %sMB" % thr)
        chunk = 1000
        self.parser.add_argument("--chunk", action="store", type=int,\
            dest="chunk", default=chunk,\
            help="Chunk size for reading Mongo docs, default %s" % chunk)
        sleep = 600
        self.parser.add_argument("--sleep", action="store", type=int, \
            dest="sleep", default=sleep, help="Sleep interval, default %s seconds" % sleep)
        self.parser.add_argument("--tstamp", action="store",\
            dest="tstamp", default="",\
            help="timestamp below which records will be removed, YYYYMMDD \
            or number with suffix 'd' for days")
        self.parser.add_argument("--stype", action="store",\
            dest="stype", default="avroio", help="Record storage type to clean-up, default avroio")

def gen_file_name(odir, compress=''):
    "Generate new file name in given odir"
    name = time.strftime("%Y%m%d_%H%M%S.avro", time.gmtime())
    if  compress:
        if  compress not in ['gz', 'bz2']:
            raise Exception('Unsupported compression: %s' % compress)
        name += '.%s' % compress
    return os.path.join(odir, name)

def file_name(odir, mdir, thr, compress):
    """
    Read content of given dir and either re-use existing file or create a new one
    based on given file size threshold. When file exceed given threshold it is
    moved into migrate area within the same given directory.
    """
    files = [f for f in os.listdir(odir) \
            if os.path.isfile(os.path.join(odir, f))]
    if  not files:
        return gen_file_name(odir, compress)

    files.sort()
    last_file = files[-1]
    fname = os.path.join(odir, last_file)
    size = os.path.getsize(fname)
    if  size < thr:
        return fname

    try:
        os.mkdir(mdir)
    except OSError:
        pass

    # move files into migration area
    shutil.move(fname, mdir)

    # remove bad file (see AvroIO.py) associated with fname
    bfname = '%s/bad/%s_bad.txt' % (os.path.dirname(fname), os.path.basename(fname))
    if  os.path.isfile(bfname):
        bfsize = os.path.getsize(bfname)
        if  not bfsize:
            os.remove(bfname)
    return file_name(odir, mdir, thr, compress)

def migrate(muri, odir, mdir, avsc, thr, compress, chunk):
    "Write data from MongoDB (muri) to avro file(s) on local file system"
    mstg = MongoStorage(muri)
    auri = avsc if avsc.startswith('avroio:') else 'avroio:%s' % avsc
    astg = AvroStorage(auri)

    # read data from MongoDB, returned mdocs is generator type
    query = {'stype': mstg.stype}
    mdocs = mstg.find(query, None) # with no fields we'll get entire docs

    # loop over provided docs and write them into avro file on local file system
    wmaids = []
    total = 0
    fsize = 0
    fname = file_name(odir, mdir, thr, compress)
    while True:
        data = [r for r in itertools.islice(mdocs, chunk)]
        total += len(data)
        if  not len(data):
            break
        ids = astg.file_write(fname, data)
        if  os.path.isfile(fname):
            fsize = os.path.getsize(fname)
        wmaids += ids

        if  ids:
            # update status attributes of docs in MongoDB
            spec = {'$set' : {'stype': astg.stype}}
            mstg.update(ids, spec)

        try:
            if  PSUTIL:
                pid = os.getpid()
                proc = psutil.Process(pid)
                mem = proc.memory_info_ex()
                rss = 'RSS:%s' % size_format(mem.rss)
            else:
                rss = ''
        except:
            rss = ''
        print(tstamp('mongo2avro'), "%s docs %s %s (%s bytes) %s" \
                % (len(ids), fname, size_format(fsize), fsize, rss))
        fname = file_name(odir, mdir, thr, compress)
    print(tstamp('mongo2avro'), "wrote %s docs out of %s" % (len(wmaids), total))

def cleanup(muri, tst, stype):
    "Cleanup data in MongoDB (muri) for given timestamp (tst)"
    time0 = time.time()
    mstg = MongoStorage(muri)
    # remove records whose type is hdfsio, i.e. already migrated to HDFS,
    # and whose time stamp is less than provided one
    query = {'stype': stype, 'wmats':{'$lt': dateformat(tst)}}
    ndocs = mstg.ndocs()
    tdocs = time.time()-time0
    print(tstamp('mongo2avro'), 'found %s docs (in %s) to be removed' % (ndocs, elapsed_time(time0)))
    time0 = time.time()
    response = mstg.remove(query)
    print(tstamp('mongo2avro'), 'remove query %s in %s' % (query, elapsed_time(time0)))

def daemon(name, opts):
    "Daemon function"
    thr = opts.thr*1024*1024 # convert input in MB into bytes
    while True:
        print(tstamp(name), 'Migrate mongodb records to avro files')
        migrate(opts.muri, opts.odir, opts.mdir, \
                opts.schema, thr, opts.compress, opts.chunk)

        print(tstamp(name), 'Cleanup MongoDB')
        cleanup(opts.muri, opts.tstamp, opts.stype)
        time.sleep(opts.sleep)

def start_new_thread(name, func, args):
    "Wrapper around standard thread.strart_new_thread call"
    threads = threading.enumerate()
    threads.sort()
    for thr in threads:
	if  name == thr.name:
	    return thr
    thr = threading.Thread(target=func, name=name, args=args)
    thr.daemon = True
    thr.start()
    return thr

def monitor(name, func, args):
    "Monitor thread for given name/func/args"
    while True:
        threads = threading.enumerate()
        threads.sort()
        found = False
        for thr in threads:
            if  name == thr.name:
                found = True
                break
        if  not found:
            print(tstamp('WARNING'), 'mongo2avro thread was not found, start new one')
            start_new_thread(name, func, (name, args))
        time.sleep(5)

def main():
    "Main function"
    optmgr = OptionParser()
    opts = optmgr.parser.parse_args()
    monitor('mongo2avro', daemon, opts)

if __name__ == '__main__':
    main()
